package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Stream

internal class IndexTest {
    @ParameterizedTest
    @MethodSource("indexProvider")
    fun `can add and search documents`(index: Index<Int>) {
        runIndexTest(index) {
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

    @ParameterizedTest
    @MethodSource("indexProvider")
    fun `updates cancel running jobs on the same document`(index: Index<Int>) {
        runIndexTest(index) {
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

    @ParameterizedTest
    @MethodSource("indexProvider")
    fun `updates should replace scheduled job if exists on the same document`(index: Index<Int>) {
        runIndexTest(index) {
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

    @ParameterizedTest
    @MethodSource("indexProvider")
    fun `searches should wait for all updates to complete`(index: Index<Int>) {
        runIndexTest(index) {
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

    companion object {
        @JvmStatic
        fun indexProvider(): Stream<Index<Int>> {
            return listOf(hashMapIndex(), segmentedIndex()).stream()
        }
    }
}
