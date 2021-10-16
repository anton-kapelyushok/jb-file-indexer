package home.pathfinder.indexing

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.helpers.NOPLogger
import java.nio.file.ClosedWatchServiceException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private val Path.canonicalPath: String get() = toFile().canonicalPath

sealed interface RootWatcherOperation {
    object StartWatching : RootWatcherOperation
    object FinishWatching : RootWatcherOperation
}

sealed interface RootWatcherStatusUpdate {
    object Started : RootWatcherStatusUpdate
    object Overflow : RootWatcherStatusUpdate
    object Error : RootWatcherStatusUpdate
    object Stopped : RootWatcherStatusUpdate
}

sealed interface FileEvent {
    data class FileUpdated(val path: String) : FileEvent
    data class FileRemoved(val path: String) : FileEvent
}

class RootWatcher2(
    root: String,
    private val output: SendChannel<FileEvent>
) {

    private val root = Paths.get(root).canonicalPath
    private val internalFiles = Channel<Any>()

    private var managedFilesWorker: Job? = null
    private var watchWorker: Job? = null

    suspend fun go(scope: CoroutineScope, started: Channel<Unit>): Job = scope.launch {
        managedFilesWorker = launchManagedFilesWorker()
        watchWorker = launchWatchWorker(started)
    }

    private fun CoroutineScope.launchManagedFilesWorker() = launch {
        val files = mutableSetOf<String>()
        for (event in internalFiles) {
            if (event is FileEvent) {
                when (event) {
                    is FileEvent.FileUpdated -> {
                        files += event.path
                        output.send(event)
                    }
                    is FileEvent.FileRemoved -> {
                        files -= event.path
                        output.send(event)
                    }
                }
            } else if (event == "removeall") {
                println(files)
                files.forEach { output.send(FileEvent.FileRemoved(it)) }
                files.clear()
            }
        }
    }

    private fun CoroutineScope.launchWatchWorker(started: SendChannel<Unit>) = launch(Dispatchers.IO) {
        var watcher: DirectoryWatcher? = null
        try {
            watcher = initializeWatcher()
            emitInitialDirectoryStructure()
            started.send(Unit)
            launch {
                try {
                    watcher.watch()
                } catch (e: ClosedWatchServiceException) {
                    // ignore
                }
            }
            awaitCancellation()
        } catch (e: Throwable) {
            if (e !is CancellationException) emitError(e)
        } finally {
            watcher?.close()
        }
    }

    suspend fun shutdown(scope: CoroutineScope) {
        watchWorker?.cancel()
        watchWorker?.join()
        internalFiles.send("removeall")
        internalFiles.close()
    }

    suspend fun restart(scope: CoroutineScope, started: SendChannel<Unit>) {
        watchWorker?.cancel()
        watchWorker?.join()
        internalFiles.send("removeall")
        watchWorker = scope.launchWatchWorker(started)
    }

    private suspend fun emitFileAdded(path: String) {
        internalFiles.send(FileEvent.FileUpdated(path))
    }

    private suspend fun emitFileRemoved(path: String) {
        internalFiles.send(FileEvent.FileRemoved(path))
    }

    private suspend fun emitOverflow() {
        // TODO
    }

    private suspend fun emitError(e: Throwable) {
        // TODO
    }


    private fun initializeWatcher(): DirectoryWatcher = DirectoryWatcher.builder()
        .logger(NOPLogger.NOP_LOGGER)
        .path(Paths.get(root))
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



