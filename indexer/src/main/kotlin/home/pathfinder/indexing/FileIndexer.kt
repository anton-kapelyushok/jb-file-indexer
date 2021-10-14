package home.pathfinder.indexing

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import java.io.File

class FileIndexer(roots: List<String>, ignorePatterns: List<String>) : Actor, SearchExact<Int> {
    private val index = HashMapIndex<Int>()
    private val fileEventChannel = Channel<FileEvent>()
    private val fileWatcherStarted = CompletableDeferred<Unit>()
    private val watcher = FileWatcher(roots, fileEventChannel, fileWatcherStarted)

    private val ignorePatternsCompiled = ignorePatterns.map { it.toRegex() }

    override fun go(scope: CoroutineScope): Job {
        return scope.launch {
            launch { watcher.go(this) }
            launch { index.go(this) }

            for (event in fileEventChannel) {
                when (event.type) {
                    FileEvent.FileEventType.ADD, FileEvent.FileEventType.MODIFY -> {

                        if (ignorePatternsCompiled.any { it.matches(event.path) }) continue
                        if (File(event.path).length() > 1_000_000) continue

                        index.updateDocument(event.path, channelFlow {
                            coroutineScope {
                                launch(Dispatchers.IO) {
                                    File(event.path).bufferedReader().use { br ->
                                        br.lineSequence().forEachIndexed { idx, line ->
                                            line
                                                .split(Regex("\\s"))
                                                .map { it.trim() }
                                                .filter { it.isNotBlank() }
                                                .forEach {
                                                    runBlocking {
                                                        channel.send(Posting(it, idx + 1))
                                                    }
                                                }
                                        }
                                    }
                                }
                            }
                        })
                    }
                    FileEvent.FileEventType.DELETE -> index.updateDocument(event.path, flow {})
                }
            }
        }
    }

    override suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<Int>> {
        fileWatcherStarted.await()
        return index.searchExact(term)
    }
}
