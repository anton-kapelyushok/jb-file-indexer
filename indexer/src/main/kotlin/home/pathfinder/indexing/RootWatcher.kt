package home.pathfinder.indexing

import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHash
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.selects.select
import org.slf4j.helpers.NOPLogger
import java.nio.file.ClosedWatchServiceException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.random.Random

private val Path.canonicalPath: String get() = toFile().canonicalPath

internal sealed interface FileEvent {
    data class FileUpdated(val path: String) : FileEvent
    data class FileRemoved(val path: String) : FileEvent
}

internal sealed interface RootWatcherEvent {
    data class ProxyEmit(val fn: suspend () -> Unit) : RootWatcherEvent
    data class RootWatcherFileEvent(val event: FileEvent) : RootWatcherEvent
    object RemoveAll : RootWatcherEvent
}

internal class RootWatcher(
    root: String,
    private val cancel: CompletableDeferred<Unit>,
) : Actor {

    val fileUpdated: ReceiveChannel<String> get() = myFileAdded
    val fileRemoved: ReceiveChannel<String> get() = myFileRemoved
    val started: ReceiveChannel<Unit> get() = myStarted
    val overflow: ReceiveChannel<Unit> get() = myOverflow
    val error: ReceiveChannel<Throwable> get() = myError
    val rootRemoved: ReceiveChannel<Unit> get() = myRootRemoved
    val stoppedWatching: ReceiveChannel<Unit> get() = myStoppedWatching

    private val myFileAdded = Channel<String>()
    private val myFileRemoved = Channel<String>()
    private val myStarted = Channel<Unit>(CONFLATED)
    private val myOverflow = Channel<Unit>(CONFLATED)
    private val myError = Channel<Throwable>(CONFLATED)
    private val myRootRemoved = Channel<Unit>(CONFLATED)
    private val myStoppedWatching = Channel<Unit>(CONFLATED)

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
                            myFileAdded.send(fileEvent.path)
                        }
                        is FileEvent.FileRemoved -> {
                            files -= fileEvent.path
                            myFileRemoved.send(fileEvent.path)
                        }
                    }
                }
                RootWatcherEvent.RemoveAll -> {
                    files.forEach {
                        myFileRemoved.send(it)
                    }
                    files.clear()
                }
                is RootWatcherEvent.ProxyEmit -> event.fn()
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
            emitRemoveAll()

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
                            if (isRegularFile) emitFileRemoved(path)
                            if (path == root) emitRootRemoved()
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
        internalEvents.send(RootWatcherEvent.ProxyEmit { myStarted.send(Unit) })

    private suspend fun emitStoppedWatching() =
        internalEvents.send(RootWatcherEvent.ProxyEmit { myStoppedWatching.send(Unit) })

    private suspend fun emitRemoveAll() =
        internalEvents.send(RootWatcherEvent.RemoveAll)

    private suspend fun emitFileAdded(path: String) =
        internalEvents.send(RootWatcherEvent.RootWatcherFileEvent(FileEvent.FileUpdated(path)))

    private suspend fun emitFileRemoved(path: String) =
        internalEvents.send(RootWatcherEvent.RootWatcherFileEvent(FileEvent.FileRemoved(path)))

    private suspend fun emitOverflow() =
        internalEvents.send(RootWatcherEvent.ProxyEmit { myOverflow.send(Unit) })

    private suspend fun emitError(e: Throwable) =
        internalEvents.send(RootWatcherEvent.ProxyEmit { myError.send(e) })

    private suspend fun emitRootRemoved() =
        internalEvents.send(RootWatcherEvent.ProxyEmit { myRootRemoved.send(Unit) })

}
