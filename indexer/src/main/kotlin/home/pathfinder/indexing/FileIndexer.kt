package home.pathfinder.indexing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.launch
import java.io.File
import java.io.FileNotFoundException

sealed interface FileIndexerEvent {
    data class AddPath(val path: String) : FileIndexerEvent
    data class RemovePath(val path: String) : FileIndexerEvent
}

class FileIndexer(ignorePatterns: List<String>) : Actor, SearchExact<Int> {
    private val index = HashMapIndex<Int>()
    private val watcherOutput = Channel<FileEvent>()
    private val mb = Channel<FileIndexerEvent>()

    private val ignorePatternsCompiled = ignorePatterns.map { it.toRegex() }

    suspend fun add(path: String) = mb.send(FileIndexerEvent.AddPath(path))
    suspend fun remove(path: String) = mb.send(FileIndexerEvent.RemovePath(path))

    override suspend fun go(scope: CoroutineScope): Job {
        return scope.launch {
            val watchers = mutableMapOf<String, RootWatcher2>()
            index.go(this)

            launch {
                for (event in watcherOutput) {
                    when (event) {
                        is FileEvent.FileRemoved -> index.updateDocument(event.path, flow {})
                        is FileEvent.FileUpdated -> {
                            if (ignorePatternsCompiled.any { it.matches(event.path) }) continue
                            if (File(event.path).length() > 1_000_000) continue
                            index.updateDocument(event.path, readPath(event.path))
                        }
                    }
                }
            }

            launch {
                for (event in mb) {
                    when (event) {
                        is FileIndexerEvent.AddPath -> {
                            val watcher = RootWatcher2(event.path, watcherOutput)
                            watchers[event.path] = watcher
                            watcher.go(scope, Channel())
                        }
                        is FileIndexerEvent.RemovePath -> {
                            watchers[event.path]?.shutdown(scope)
                        }
                    }
                }
            }
        }
    }

    override suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<Int>> {
        println("searchExact")
        return index.searchExact(term)
    }

    private fun readPath(path: String) = flow {
        try {
            File(path).bufferedReader().use { br ->
                br.lineSequence().forEachIndexed { idx, line ->
                    line
                        .split(Regex("\\s"))
                        .map { it.trim() }
                        .filter { it.isNotBlank() }
                        .forEach {
                            emit(Posting(it, idx + 1))
                        }
                }
            }
        } catch (e: FileNotFoundException) {
            // ignore
        }
    }.flowOn(Dispatchers.IO)
}
