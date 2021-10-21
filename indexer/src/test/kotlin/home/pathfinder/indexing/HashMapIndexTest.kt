package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean

internal class HashMapIndexTest {

    @Test
    fun `can add and search documents`() {
        runIndexTest { index ->
            index.updateDocument("doc1", flow {
                emit(Posting("term1", 1))
                emit(Posting("term2", 2))
            })

            index.updateDocument("doc2", flow {
                emit(Posting("term2", 1))
                emit(Posting("term3", 2))
            })

            assertThat(index.searchExactOrdered("term1")).isEqualTo(
                listOf(
                    SearchResultEntry("doc1", "term1", 1),
                )
            )
            assertThat(index.searchExactOrdered("term2")).isEqualTo(
                listOf(
                    SearchResultEntry("doc1", "term2", 2),
                    SearchResultEntry("doc2", "term2", 1),
                )
            )
            assertThat(index.searchExactOrdered("term3")).isEqualTo(
                listOf(
                    SearchResultEntry("doc2", "term3", 2),
                )
            )
        }
    }

    @Test
    fun `updates cancel running jobs on the same document`() {
        runIndexTest { index ->
            val firstUpdateStarted = Channel<Unit>(CONFLATED)

            index.updateDocument("doc1", flow {
                firstUpdateStarted.send(Unit)
                emit(Posting("term1", 1))
                delay(1_000_000)
            })

            firstUpdateStarted.receive()

            index.updateDocument("doc1", flow {
                emit(Posting("term2", 1))
                emit(Posting("term3", 1))
            })

            assertThat(index.searchExactOrdered("term1")).isEmpty()
            assertThat(index.searchExactOrdered("term2")).isEqualTo(
                listOf(
                    SearchResultEntry("doc1", "term2", 1),
                )
            )
            assertThat(index.searchExactOrdered("term3")).isEqualTo(
                listOf(
                    SearchResultEntry("doc1", "term3", 1),
                )
            )
        }
    }

    @Test
    fun `updates should replace scheduled job if exists on the same document`() {
        runIndexTest { index ->
            val releaseBarrier = CompletableDeferred<Unit>()

            // fill up workers
            repeat(100) {
                index.updateDocument("filler$it", flow { releaseBarrier.await() })
            }

            val firstStarted = AtomicBoolean(false)

            index.updateDocument("doc1", flow {
                firstStarted.set(false)
                emit(Posting("term1", 1))
                delay(1_000_000)
            })

            index.updateDocument("doc1", flow {
                emit(Posting("term1", 2))
            })

            releaseBarrier.complete(Unit)

            assertThat(firstStarted.get()).isEqualTo(false)
            assertThat(index.searchExactOrdered("term1")).isEqualTo(
                listOf(
                    SearchResultEntry("doc1", "term1", 2),
                )
            )
        }
    }

    @Test
    fun `searches should wait for all updates to complete`() {
        runIndexTest { index ->
            val firstUpdateBarrierRelease = CompletableDeferred<Unit>()
            index.updateDocument("doc1", flow {
                firstUpdateBarrierRelease.await()
                emit(Posting("term1", 1))
            })

            val searchStarted = CompletableDeferred<Unit>()
            val searchDeferred = async {
                searchStarted.complete(Unit)
                index.searchExactOrdered("term1")
            }

            val secondUpdateStarted = CompletableDeferred<Unit>()
            launch {
                searchStarted.await()
                secondUpdateStarted.complete(Unit)
                index.updateDocument("doc2", flow {
                    emit(Posting("term1", 2))
                })
            }

            secondUpdateStarted.await()
            firstUpdateBarrierRelease.complete(Unit)

            assertThat(searchDeferred.await()).isEqualTo(
                listOf(
                    SearchResultEntry("doc1", "term1", 1),
                    SearchResultEntry("doc2", "term1", 2),
                )
            )
        }
    }

    @Test
    fun `should not run update when search is running`() {
        runIndexTest { index ->
            index.updateDocument("doc1", flow {
                emit(Posting("term1", 1))
                emit(Posting("term1", 2))
            })

            val searchStarted = CompletableDeferred<Unit>()
            val searchJob = launch {
                index.searchExact("term1").collect {
                    searchStarted.complete(Unit)
                    delay(1_000_000)
                }
            }

            searchStarted.await()
            index.updateDocument("doc2", flow {})

            delay(100)
            assertThat(index.state.value.pendingUpdates).isEqualTo(1)
            searchJob.cancel()
        }
    }

    private suspend fun HashMapIndex<Int>.searchExactOrdered(term: Term): List<SearchResultEntry<Int>> {
        return this.searchExact(term).toList().sortedWith(compareBy({ it.documentName }, { it.term }, { it.termData }))
    }

    private fun <T> runIndexTest(fn: suspend CoroutineScope.(index: HashMapIndex<Int>) -> T): T {
        return runBlocking {
            withTimeout(1000) {
                val index = HashMapIndex<Int>()
                val indexJob = launch { index.go(this) }

                try {
                    fn(index)
                } finally {
                    indexJob.cancel()
                }
            }
        }
    }
}
