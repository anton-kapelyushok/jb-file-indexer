package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.isEqualTo
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test

class HashMapIndexTest {
    @Test
    fun `should not run update when search is running`() {
        val index = hashMapIndex()
        runIndexTest(index) {
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
            index.updateDocument("doc2", flow {
                emit(Posting("term1", 3))
            })

            delay(100)
            assertThat(index.state.value.pendingUpdates).isEqualTo(1)
            searchJob.cancel()

            assertThat(index.searchExactOrdered("term1")).isEqualTo(
                listOf(
                    SearchResultEntry("doc1", "term1", 1),
                    SearchResultEntry("doc1", "term1", 2),
                    SearchResultEntry("doc2", "term1", 3),
                )
            )
        }
    }
}