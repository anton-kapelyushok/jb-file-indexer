package home.pathfinder.indexing

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.selects.select
import org.slf4j.helpers.NOPLogger
import java.nio.file.ClosedWatchServiceException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private val Path.canonicalPath: String get() = toFile().canonicalPath

sealed interface FileEvent {
    data class FileUpdated(val path: String) : FileEvent
    data class FileRemoved(val path: String) : FileEvent
}

sealed interface RootWatcherEvent {
    data class RootWatcherFileEvent(val event: FileEvent) : RootWatcherEvent
    object RemoveAll : RootWatcherEvent
}

class RootWatcher(
    root: String,
    private val output: SendChannel<FileEvent>,
    private val started: SendChannel<Unit>,
    private val overflow: SendChannel<Unit>,
    private val cancel: ReceiveChannel<Unit>
) : Actor {

    private val root = Paths.get(root).canonicalPath
    private val internalEvents = Channel<RootWatcherEvent>()

    override suspend fun go(
        scope: CoroutineScope
    ): Job = scope.launch {
        launchStateHolder()
        launchWatchWorker()
    }

    private fun CoroutineScope.launchStateHolder() = launch {
        val files = mutableSetOf<String>()
        for (event in internalEvents) {
            when (event) {
                is RootWatcherEvent.RootWatcherFileEvent -> {
                    when (val fileEvent = event.event) {
                        is FileEvent.FileUpdated -> {
                            files += fileEvent.path
                            output.send(fileEvent)
                        }
                        is FileEvent.FileRemoved -> {
                            files -= fileEvent.path
                            output.send(fileEvent)
                        }
                    }
                }
                RootWatcherEvent.RemoveAll -> {
                    files.forEach {
//                        println("remove $it")
                        output.send(FileEvent.FileRemoved(it))
                    }
                    files.clear()
                }
            }
        }
    }

    private fun CoroutineScope.launchWatchWorker() =
        launch {
            val job = launch(Dispatchers.IO) {
                var watcher: DirectoryWatcher? = null
                try {
                    watcher = runInterruptible { initializeWatcher() }
                    yield()
                    runInterruptible { emitInitialDirectoryStructure() }
                    started.send(Unit)
                    launch {
                        try {
                            runInterruptible { watcher.watch() }
                        } catch (e: ClosedWatchServiceException) {
                            // ignore
                        } catch (e: Throwable) {
                            // TODO
                            println("first $e")
                            throw e
                        }
                    }
                    awaitCancellation()
                } catch (e: Throwable) {
                    if (e !is CancellationException) emitError(e)
                    // TODO
                    println("second $e")
                    awaitCancellation()
                } finally {
                    watcher?.close()
                }
            }

            select<Unit> {
                job.onJoin {}
                cancel.onReceive {
                    job.cancel()
                    job.join()
                }
            }

            internalEvents.send(RootWatcherEvent.RemoveAll)
            internalEvents.close()
        }

    private suspend fun emitFileAdded(path: String) {
//        println("add $path")
        internalEvents.send(RootWatcherEvent.RootWatcherFileEvent(FileEvent.FileUpdated(path)))
    }

    private suspend fun emitFileRemoved(path: String) {
//        println("remove $path")
        internalEvents.send(RootWatcherEvent.RootWatcherFileEvent(FileEvent.FileRemoved(path)))
    }

    private suspend fun emitOverflow() {
        overflow.send(Unit)
    }

    private suspend fun emitError(e: Throwable) {
        // TODO
    }

    private fun initializeWatcher(): DirectoryWatcher = DirectoryWatcher.builder()
        .logger(NOPLogger.NOP_LOGGER)
        .path(Paths.get(root))
        .fileHasher {
            // A hack to fast cancel watcher.build()
            if (Thread.interrupted()) {
                throw InterruptedException()
            }
            null
        }
        .listener { event ->
            runBlocking {
                val path = event.path().canonicalPath
                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                when (event.eventType()) {
                    DirectoryChangeEvent.EventType.CREATE -> emitFileAdded(path)
                    DirectoryChangeEvent.EventType.MODIFY -> emitFileAdded(path)
                    DirectoryChangeEvent.EventType.DELETE -> emitFileRemoved(path)
                    DirectoryChangeEvent.EventType.OVERFLOW -> emitOverflow()
                }
            }
        }
        .build()

    private fun emitInitialDirectoryStructure() {
        Files.walk(Paths.get(root))
            .use { stream ->
                stream.filter(Files::isRegularFile)
                    .forEach {
                        runBlocking {
                            emitFileAdded(it.canonicalPath)
                        }
                    }
            }
    }
}
