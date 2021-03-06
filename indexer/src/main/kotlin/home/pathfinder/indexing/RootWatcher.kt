package home.pathfinder.indexing

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
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
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.random.Random

internal sealed interface RootWatcherEvent {
    sealed interface RootWatcherLifeCycleEvent : RootWatcherEvent
    sealed interface RootWatcherFileEvent : RootWatcherEvent {
        val path: String
    }

    object Overflown : RootWatcherLifeCycleEvent
    object Initialized : RootWatcherLifeCycleEvent
    data class Failed(val exception: Throwable) : RootWatcherLifeCycleEvent
    object RootDeleted : RootWatcherLifeCycleEvent
    object StoppedWatching : RootWatcherLifeCycleEvent
    object Stopped : RootWatcherLifeCycleEvent

    data class FileUpdated(override val path: String) : RootWatcherFileEvent
    data class FileDeleted(override val path: String) : RootWatcherFileEvent
}

internal class RootWatcher(
    watchedRoot: WatchedRoot,
    private val rwInitialEmitFromFileHasherHackEnabled: Boolean = false,
) : Actor {

    val events = Channel<RootWatcherEvent>()

    private val root = Paths.get(watchedRoot.root).canonicalPath
    private val ignoredRoots = PathTree(watchedRoot.ignoredRoots.map { Paths.get(it).canonicalPath })
    private val actualRoots = PathTree(watchedRoot.actualRoots.map { Paths.get(it).canonicalPath })
    private val internalEvents = Channel<RootWatcherEvent>()

    private val initializing = AtomicBoolean(true)
    private val cancel = CompletableDeferred<Unit>()

    fun cancel() = cancel.complete(Unit)

    override suspend fun go(
        scope: CoroutineScope
    ): Job = scope.launch {
        launchStateHolder()
        launchWatchWorker()
    }

    private fun CoroutineScope.launchStateHolder() = launch {
        try {
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
                        events.send(RootWatcherEvent.Stopped)
                        events.close()
                    }
                    else -> {
                        events.send(event)
                    }
                }
            }
        } finally {
            internalEvents.cancel()
        }
    }

    // I literally have launch(Dispatchers.IO)
    @Suppress("BlockingMethodInNonBlockingContext")
    private fun CoroutineScope.launchWatchWorker() =
        launch(Dispatchers.IO) {
            var watcher: DirectoryWatcher? = null
            try {
                val watcherBuildJob = async {
                    try {
                        runInterruptible { initializeWatcher(cancel) }
                    } catch (e: Throwable) {
                        if (isActive) emitError(e)
                        null
                    }
                }

                select<Unit> {
                    cancel.onAwait {
                        watcherBuildJob.cancel()
                        watcherBuildJob.join()
                    }
                    watcherBuildJob.onJoin {}
                }

                watcher = watcherBuildJob.await()
                initializing.set(false)

                if (!cancel.isCompleted && watcher != null) {
                    val job = launch(Dispatchers.IO) {
                        try {
                            if (!rwInitialEmitFromFileHasherHackEnabled) runInterruptible { emitInitialDirectoryStructure() }
                            emitStarted()
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

            internalEvents.close()
        }


    private fun initializeWatcher(cancel: CompletableDeferred<Unit>): DirectoryWatcher {
        return DirectoryWatcher.builder()
            .logger(NOPLogger.NOP_LOGGER)
            .path(Paths.get(root))
            .fileHasher { path ->
                // A hack to fast cancel watcher.build()
                if (Thread.interrupted()) {
                    throw InterruptedException()
                }

                // A hack to speed up initialization process
                if (rwInitialEmitFromFileHasherHackEnabled && initializing.get()) {
                    if (!isIgnored(path))
                        runBlocking {
                            emitFileAdded(path.toString())
                        }
                }

                FileHash.fromBytes(Random.nextBytes(16))
            }
            .listener(object : DirectoryChangeListener {
                override fun onEvent(event: DirectoryChangeEvent) {
                    debugLog("listener event: $event")
                    runBlocking {
                        val path = event.path().canonicalPath
                        if (isIgnored(event.path())) return@runBlocking

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

                override fun onException(e: Exception) {
                    runBlocking {
                        emitError(e)
                        cancel.complete(Unit)
                    }
                }
            })
            .build()
    }

    private fun emitInitialDirectoryStructure() {
        Files.walk(Paths.get(root))
            .use { stream ->
                stream
                    .filter(Files::isRegularFile)
                    .filter { !isIgnored(it) }
                    .forEach {
                        runBlocking {
                            emitFileAdded(it.toString())
                        }
                    }
            }
    }

    private fun isIgnored(path: Path): Boolean {
        val canonicalPath = path.toString()

        if (ignoredRoots.containsPathOrItsParent(canonicalPath)) return true
        if (!actualRoots.containsPathOrItsParent(canonicalPath)) return true

        if (path.exists() && path.fileSize() > 10_000_000) {
            return true
        }

        return false
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
}
