package home.pathfinder.indexing

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.helpers.NOPLogger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


data class FileEvent(
    val type: FileEventType,
    val path: String,
) {
    enum class FileEventType { ADD, MODIFY, DELETE }
}

val Path.canonicalPath: String get() = this.toFile().canonicalPath

class FileWatcher(
    roots: List<String>,
    private val change: SendChannel<FileEvent>,
    private val started: CompletableDeferred<Unit>
) : Actor {
    private val roots = roots.map { Paths.get(it).normalize() }.distinct()
    override fun go(scope: CoroutineScope): Job {
        return try {
            scope.launch(Dispatchers.IO) {

                val startWatcher = CompletableDeferred<Unit>()

                val watcher = DirectoryWatcher.builder()
                    .logger(NOPLogger.NOP_LOGGER)
                    .paths(roots)
                    .listener { event ->
                        runBlocking {
                            startWatcher.await()
                            val path = event.path().canonicalPath
                            when (event.eventType()) {
                                DirectoryChangeEvent.EventType.CREATE -> {
                                    change.send(FileEvent(FileEvent.FileEventType.ADD, path))
                                }
                                DirectoryChangeEvent.EventType.MODIFY -> {
                                    change.send(FileEvent(FileEvent.FileEventType.MODIFY, path))
                                }
                                DirectoryChangeEvent.EventType.DELETE -> {
                                    change.send(FileEvent(FileEvent.FileEventType.DELETE, path))
                                }
                                else -> error("unexpected? recreate index?")
                            }
                        }
                    }
                    .build()

                roots.forEach { path ->
                    Files.walk(path).use { stream ->
                        stream.filter(Files::isRegularFile)
                            .forEach {
                                runBlocking { change.send(FileEvent(FileEvent.FileEventType.ADD, it.canonicalPath)) }
                            }
                    }
                }

                startWatcher.complete(Unit)
                started.complete(Unit)
                watcher.watch()
            }
        } catch (e: Throwable) {
            change.close(e)
            throw e
        }
    }
}
