package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.createDirectory
import kotlin.io.path.createFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.writeLines

class FileIndexerTest {

    @Test
    fun `happy path`() {
        fileSystemTest { workingDirectory ->
            val firstRoot = Paths.get(workingDirectory.toString(), "poupa")
            firstRoot.createDirectory()

            val poupaFile = Paths.get(firstRoot.toString(), "poupa.txt").also { it.createFile() }
            val loupaFile = Paths.get(firstRoot.toString(), "loupa.txt").also { it.createFile() }

            poupaFile.writeLines(sequence {
                yield("term1 term2 term3")
                yield("term2 term3 term4")
            })

            loupaFile.writeLines(sequence {
                yield("term3 term4 term5")
                yield("term6 term7 term8")
            })

            val secondRoot = Paths.get(workingDirectory.toString(), "loupa")
            secondRoot.createDirectory()

            val volobuevFile = Paths.get(secondRoot.toString(), "volobuev.txt").also { it.createFile() }
            val mechFile = Paths.get(secondRoot.toString(), "mech.txt")

            volobuevFile.writeLines(sequence {
                yield("term1 term3 term5")
                yield("term1 term7 term13")
            })

            withFileIndexer { indexer ->
                // search on empty index
                assertThat(indexer.searchExactOrdered("term1")).isEmpty()

                // root updates are recognized immediately
                indexer.updateContentRoots(setOf(firstRoot.toString()))
                assertThat(indexer.searchExactOrdered("term3")).containsExactlyInAnyOrder(
                    SearchResultEntry("poupa.txt", "term3", 1),
                    SearchResultEntry("poupa.txt", "term3", 2),
                    SearchResultEntry("loupa.txt", "term3", 1),
                )

                indexer.updateContentRoots(setOf(firstRoot.toString(), secondRoot.toString()))
                assertThat(indexer.searchExactOrdered("term3")).containsExactlyInAnyOrder(
                    SearchResultEntry("poupa.txt", "term3", 1),
                    SearchResultEntry("poupa.txt", "term3", 2),
                    SearchResultEntry("loupa.txt", "term3", 1),
                    SearchResultEntry("volobuev.txt", "term3", 1),
                )

                // new files are detected
                waitUntilUpdateRecognized(indexer) {
                    mechFile.writeLines(sequence {
                        yield("term2 term4 term6")
                        yield("term3 term3 term12")
                    })
                }
                assertThat(indexer.searchExactOrdered("term3")).containsExactlyInAnyOrder(
                    SearchResultEntry("poupa.txt", "term3", 1),
                    SearchResultEntry("poupa.txt", "term3", 2),
                    SearchResultEntry("loupa.txt", "term3", 1),
                    SearchResultEntry("volobuev.txt", "term3", 1),
                    SearchResultEntry("mech.txt", "term3", 2),
                )

                // file updates are detected
                waitUntilUpdateRecognized(indexer) {
                    mechFile.writeLines(sequence {
                        yield("term3 term3 term12")
                    })
                }
                assertThat(indexer.searchExactOrdered("term3")).containsExactlyInAnyOrder(
                    SearchResultEntry("poupa.txt", "term3", 1),
                    SearchResultEntry("poupa.txt", "term3", 2),
                    SearchResultEntry("loupa.txt", "term3", 1),
                    SearchResultEntry("volobuev.txt", "term3", 1),
                    SearchResultEntry("mech.txt", "term3", 1),
                )

                // file deletes are detected
                waitUntilUpdateRecognized(indexer) {
                    volobuevFile.deleteExisting()
                }
                assertThat(indexer.searchExactOrdered("term3")).containsExactlyInAnyOrder(
                    SearchResultEntry("poupa.txt", "term3", 1),
                    SearchResultEntry("poupa.txt", "term3", 2),
                    SearchResultEntry("loupa.txt", "term3", 1),
                    SearchResultEntry("mech.txt", "term3", 1),
                )

                // root deletes are handled
                waitUntilUpdateRecognized(indexer) {
                    mechFile.deleteExisting()
                    secondRoot.deleteExisting()
                }

                assertThat(indexer.searchExactOrdered("term3")).containsExactlyInAnyOrder(
                    SearchResultEntry("poupa.txt", "term3", 1),
                    SearchResultEntry("poupa.txt", "term3", 2),
                    SearchResultEntry("loupa.txt", "term3", 1),
                )

                waitUntilIndexState(indexer) { fileIndexerStatus ->
                    fileIndexerStatus.watcherStates.any { (path, info) ->
                        info.status == RootWatcherStateInfo.Status.Failed
                                && info.exception is RootWatcherState.RootDeletedException
                                && File(path).canonicalPath == secondRoot.toFile().canonicalPath
                    }
                }

                // failures are removed from status info on root removal
                indexer.updateContentRoots(setOf(firstRoot.toString()))
                waitUntilIndexState(indexer) { fileIndexerStatus ->
                    fileIndexerStatus.watcherStates.all { (_, info) ->
                        info.status != RootWatcherStateInfo.Status.Failed
                    }
                }
            }
        }
    }

    private suspend fun <T> CoroutineScope.withFileIndexer(fn: suspend (FileIndexer) -> T): T {
        val indexer = fileIndexer()
        val job = launch { indexer.go(this) }
        return try {
            fn(indexer)
        } finally {
            job.cancel()
        }
    }

    private suspend fun FileIndexer.searchExactOrdered(term: Term): List<SearchResultEntry<Int>> {
        return this.searchExact(term).toList()
            .map { SearchResultEntry(it.documentName.split(File.separator).last(), it.term, it.termData) }
            .sortedWith(compareBy({ it.documentName }, { it.term }, { it.termData }))
    }

    private suspend fun waitUntilIndexRecognizesUpdate(fileIndexer: FileIndexer) {
        fileIndexer.state.takeWhile { it.indexInfo.pendingUpdates == 0 && it.indexInfo.runningUpdates == 0 }.collect()
    }

    private suspend fun CoroutineScope.waitUntilUpdateRecognized(
        fileIndexer: FileIndexer,
        fn: suspend () -> Unit
    ) {
        val updateRecognized = launch { waitUntilIndexRecognizesUpdate(fileIndexer) }
        fn()
        updateRecognized.join()
    }

    private suspend fun CoroutineScope.waitUntilIndexState(
        fileIndexer: FileIndexer,
        fn: (statusInfo: FileIndexerStatusInfo) -> Boolean
    ) {
        fileIndexer.state.takeWhile { !fn(it) }
    }
}
