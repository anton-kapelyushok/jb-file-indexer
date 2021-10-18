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
    private val cancel: CompletableDeferred<Unit>,
) : Actor {

    val fileEvents: ReceiveChannel<FileEvent> get() = myFileEvents
    val started: ReceiveChannel<Unit> get() = myStarted
    val overflow: ReceiveChannel<Unit> get() = myOverflow
    val error: ReceiveChannel<Throwable> get() = myError
    val rootRemoved: ReceiveChannel<Unit> get() = myRootRemoved

    private val myFileEvents = Channel<FileEvent>()
    private val myStarted = Channel<Unit>(CONFLATED)
    private val myOverflow = Channel<Unit>(CONFLATED)
    private val myError = Channel<Throwable>(CONFLATED)
    private val myRootRemoved = Channel<Unit>(CONFLATED)

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
                            myFileEvents.send(fileEvent)
                        }
                        is FileEvent.FileRemoved -> {
                            files -= fileEvent.path
                            myFileEvents.send(fileEvent)
                        }
                    }
                }
                RootWatcherEvent.RemoveAll -> {
                    files.forEach {
//                        println("remove $it")
                        myFileEvents.send(FileEvent.FileRemoved(it))
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
                    myStarted.send(Unit)
                    runInterruptible {
                        try {
                            println("watching")
                            watcher.watch()
                            println("after watch")
                        } catch (e: ClosedWatchServiceException) {
                            // ignore
                        }
                    }
                } catch (e: Throwable) {
                    if (isActive) emitError(e)
                } finally {
                    watcher?.close()
                }
            }

            select<Unit> {
                cancel.onAwait { job.cancel() }
                job.onJoin {}
            }

            internalEvents.send(RootWatcherEvent.RemoveAll)
            internalEvents.close()

            cancel.await()
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
        myOverflow.send(Unit)
    }

    private suspend fun emitError(e: Throwable) {
        myError.send(e)
    }

    private suspend fun emitRootRemoved() {
        myRootRemoved.send(Unit)
    }

    private fun initializeWatcher(): DirectoryWatcher = DirectoryWatcher.builder()
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
                println(event)
                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                when (event.eventType()) {
                    DirectoryChangeEvent.EventType.CREATE -> {
                        if (isRegularFile) emitFileAdded(path)
                    }
                    DirectoryChangeEvent.EventType.MODIFY -> {
                        if (isRegularFile) emitFileAdded(path)
                    }
                    DirectoryChangeEvent.EventType.DELETE -> {
                        println(isRegularFile)
                        if (isRegularFile) emitFileRemoved(path)
                        if (path == root) emitRootRemoved()
                    }
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
