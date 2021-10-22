package home.pathfinder.indexing

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHash
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import org.slf4j.helpers.NOPLogger
import java.nio.file.ClosedWatchServiceException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.random.Random

private val Path.canonicalPath: String get() = toFile().canonicalPath

internal sealed interface RootWatcherEvent {
    sealed interface RootWatcherLifeCycleEvent : RootWatcherEvent
    sealed interface RootWatcherFileEvent : RootWatcherEvent

    object Overflown : RootWatcherLifeCycleEvent
    object Initialized : RootWatcherLifeCycleEvent
    data class Failed(val exception: Throwable) : RootWatcherLifeCycleEvent
    object RootDeleted : RootWatcherLifeCycleEvent
    object StoppedWatching : RootWatcherLifeCycleEvent
    object Stopped : RootWatcherLifeCycleEvent

    data class FileUpdated(val path: String) : RootWatcherFileEvent
    data class FileDeleted(val path: String) : RootWatcherFileEvent
}

internal class RootWatcher(
    root: String,
    private val cancel: CompletableDeferred<Unit>,
) : Actor {

    val events = Channel<RootWatcherEvent>()

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
                is RootWatcherEvent.FileUpdated -> {
                    files += event.path
                    events.send(event)
                }
                is RootWatcherEvent.FileDeleted -> {
                    files -= event.path
                    events.send(event)
                }
                RootWatcherEvent.StoppedWatching -> {
                    events.send(event)
                    files.forEach {
                        events.send(RootWatcherEvent.FileDeleted(it))
                    }
                    files.clear()
                }
                RootWatcherEvent.Stopped -> {
                    events.send(event)
                    events.close()
                }
                else -> {
                    events.send(event)
                }
            }
        }
    }

    // I literally have launch(Dispatchers.IO)
    @Suppress("BlockingMethodInNonBlockingContext")
    private fun CoroutineScope.launchWatchWorker() =
        launch(Dispatchers.IO) {
            var watcher: DirectoryWatcher? = null
            try {
                watcher = runInterruptible {
                    initializeWatcher()
                }.getOrElse {
                    if (isActive) emitError(it)
                    null
                }

                if (watcher != null) {
                    emitInitialDirectoryStructure()
                    emitStarted()

                    val job = launch(Dispatchers.IO) {
                        try {
                            runInterruptible {
                                watcher.watch()
                            }
                        } catch (e: ClosedWatchServiceException) {
                            // ignore
                        } catch (e: Throwable) {
                            if (isActive) emitError(e)
                        } finally {
                            watcher.close()
                        }
                    }

                    select<Unit> {
                        cancel.onAwait {
                            job.cancel()
                            watcher.close()
                            job.join()
                        }
                        job.onJoin {}
                    }
                }

            } catch (e: Throwable) {
                if (isActive) emitError(e)
            } finally {
                watcher?.close()
            }

            emitStoppedWatching()
            emitStopped()

            internalEvents.close()
        }


    private fun initializeWatcher(): Result<DirectoryWatcher> = kotlin.runCatching {
        DirectoryWatcher.builder()
            .logger(NOPLogger.NOP_LOGGER)
            .path(Paths.get(root))
            .fileHasher {
                // A hack to fast cancel watcher.build()
                if (Thread.interrupted()) {
                    throw InterruptedException()
                }
                FileHash.fromBytes(Random.Default.nextBytes(16))
            }
            .listener { event ->
                runBlocking {
                    val path = event.path().canonicalPath
                    val isRegularFile = !event.isDirectory
                    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                    when (event.eventType()) {
                        DirectoryChangeEvent.EventType.CREATE -> {
                            if (isRegularFile) emitFileAdded(path)
                        }
                        DirectoryChangeEvent.EventType.MODIFY -> {
                            if (isRegularFile) emitFileAdded(path)
                        }
                        DirectoryChangeEvent.EventType.DELETE -> {
                            if (isRegularFile) emitFileDeleted(path)
                            if (path == root) emitRootDeleted()
                        }
                        DirectoryChangeEvent.EventType.OVERFLOW -> emitOverflow()
                    }
                }
            }
            .build()
    }

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

    private suspend fun emitStarted() =
        internalEvents.send(RootWatcherEvent.Initialized)

    private suspend fun emitStoppedWatching() =
        internalEvents.send(RootWatcherEvent.StoppedWatching)

    private suspend fun emitFileAdded(path: String) =
        internalEvents.send(RootWatcherEvent.FileUpdated(path))

    private suspend fun emitFileDeleted(path: String) =
        internalEvents.send(RootWatcherEvent.FileDeleted(path))

    private suspend fun emitOverflow() =
        internalEvents.send(RootWatcherEvent.Overflown)

    private suspend fun emitError(e: Throwable) =
        internalEvents.send(RootWatcherEvent.Failed(e))

    private suspend fun emitRootDeleted() =
        internalEvents.send(RootWatcherEvent.RootDeleted)

    private suspend fun emitStopped() =
        internalEvents.send(RootWatcherEvent.Stopped)
}
