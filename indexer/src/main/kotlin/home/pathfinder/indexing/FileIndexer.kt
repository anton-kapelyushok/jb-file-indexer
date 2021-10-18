package home.pathfinder.indexing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import java.io.File
import java.io.FileNotFoundException

interface FileIndexer {
    suspend fun go(scope: CoroutineScope)

    /**
     * Updates current roots.
     * Function call returns (almost) immediately, update is scheduled.
     * Search is blocked until update comes through.
     */
    suspend fun updateContentRoots(newRoots: Set<String>)

    suspend fun searchExact(term: String): Flow<SearchResultEntry<Int>>

    /**
     * Contains information about index state and errors
     */
    val stateFlow: StateFlow<Any>
}

typealias FileIndexerState = Any

sealed interface RootWatcherState {
    data class Initializing(override val cancel: Channel<Unit>) : RootWatcherState, Cancelable
    data class Running(override val cancel: Channel<Unit>) : RootWatcherState, Cancelable
    data class Failed(override val cancel: Channel<Unit>, val error: Throwable) : RootWatcherState, Cancelable
    object Canceling : RootWatcherState

    interface Cancelable {
        val cancel: Channel<Unit>
    }
}

sealed interface IndexerEvent {
    data class UpdateRoots(val roots: Set<String>) : IndexerEvent
    data class WatcherOverflown(val path: String) : IndexerEvent
    data class RootInitialized(val path: String) : IndexerEvent
    data class WatcherFinished(val path: String) : IndexerEvent
    data class WatcherFailed(val path: String, val exception: Throwable) : IndexerEvent
    sealed interface Search : IndexerEvent {
        val ready: Channel<Unit>

        data class Exact(val term: String, override val ready: Channel<Unit>) : Search
    }
}

class FileIndexerImpl : FileIndexer {

    private val index = HashMapIndex<Int>()

    private val rootWatcherStates = mutableMapOf<String, RootWatcherState>()
    private var watchedRoots = mutableSetOf<String>()

    private val indexerEvents = Channel<IndexerEvent>()
    private val fileEvents = Channel<FileEvent>(UNLIMITED)

    override suspend fun go(scope: CoroutineScope) {
        scope.launch {
            launch { index.go(this) }

            launch {
                for (msg in fileEvents) {
                    when (msg) {
                        is FileEvent.FileRemoved -> index.removeDocument(msg.path)
                        is FileEvent.FileUpdated -> index.updateDocument(msg.path, readPath(msg.path))
                    }
                }
            }

            launch {
                val workerScope = this

                while (true) {
                    select<Unit> {
                        indexerEvents.onReceive { request ->
                            println("Received updateRequests $request")
                            when (request) {
                                is IndexerEvent.UpdateRoots -> {
                                    val failedRoots =
                                        rootWatcherStates.filter { (_, v) -> v is RootWatcherState.Failed }
                                            .map { (k, _) -> k }

                                    val watchersToCancel = watchedRoots - request.roots + failedRoots

                                    watchedRoots = request.roots.toMutableSet()
                                    watchersToCancel.forEach { cancelWatcher(it) }
                                }
                                is IndexerEvent.RootInitialized -> {
                                    when (val rootState = rootWatcherStates[request.path]) {
                                        is RootWatcherState.Initializing -> {
                                            rootWatcherStates[request.path] = RootWatcherState.Running(rootState.cancel)
                                        }
                                    }
                                }
                                is IndexerEvent.WatcherOverflown -> {
                                    cancelWatcher(request.path)
                                }
                                is IndexerEvent.WatcherFinished -> {
                                    rootWatcherStates -= request.path
                                }
                                is IndexerEvent.Search.Exact -> {
                                    request.ready.send(Unit)
                                }
                                is IndexerEvent.WatcherFailed -> {
                                    when (val rootState = rootWatcherStates[request.path]) {
                                        is RootWatcherState.Cancelable -> {
                                            rootWatcherStates[request.path] =
                                                RootWatcherState.Failed(rootState.cancel, request.exception)
                                        }
                                    }
                                }
                            }

                            launchMissingWatchers(workerScope)
                            updateIndexLockingStatus()
                            println("$rootWatcherStates")
                        }
                    }
                }
            }
        }
    }

    override val stateFlow: StateFlow<FileIndexerState> get() = null!!

    override suspend fun updateContentRoots(newRoots: Set<String>) {
        indexerEvents.send(IndexerEvent.UpdateRoots(newRoots))
    }

    override suspend fun searchExact(term: String): Flow<SearchResultEntry<Int>> = flow {
        val ready = Channel<Unit>(CONFLATED)
        indexerEvents.send(IndexerEvent.Search.Exact(term, ready))
        ready.receive()
        index.searchExact(term).onEach { emit(it) }.collect()
    }

    private suspend fun cancelWatcher(root: String) {
        when (val rootState = rootWatcherStates[root]) {
            is RootWatcherState.Cancelable -> {
                rootState.cancel.send(Unit)
                rootWatcherStates[root] = RootWatcherState.Canceling
            }
        }
    }

    private suspend fun launchMissingWatchers(scope: CoroutineScope) {
        if (rootWatcherStates.values.any { it is RootWatcherState.Canceling }) return

        val updatesToRun = watchedRoots
            .filter { it !in rootWatcherStates }

        updatesToRun.forEach {
            launchWatcher(scope, it)
        }
    }

    private suspend fun launchWatcher(scope: CoroutineScope, path: String) {
        assert(rootWatcherStates[path] == null)

        println("Starting $path watcher")

        val overflow = Channel<Unit>(CONFLATED)
        val cancel = Channel<Unit>(CONFLATED)
        val started = Channel<Unit>(CONFLATED)
        val error = Channel<Throwable>(CONFLATED)

        val watcher = RootWatcher(
            root = path,
            output = fileEvents,
            started = started,
            overflow = overflow,
            error = error,
            cancel = cancel
        )

        rootWatcherStates[path] = RootWatcherState.Initializing(cancel)

        scope.launch {
            try {
                var running = true
                val job = watcher.go(this)
                while (running) {
                    select<Unit> {
                        started.onReceive {
                            indexerEvents.send(IndexerEvent.RootInitialized(path))
                        }
                        overflow.onReceive {
                            indexerEvents.send(IndexerEvent.WatcherOverflown(path))
                        }
                        error.onReceive {
                            indexerEvents.send(IndexerEvent.WatcherFailed(path, it))
                        }
                        job.onJoin { running = false }
                    }
                }
            } finally {
                indexerEvents.send(IndexerEvent.WatcherFinished(path))
            }
        }
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

    private suspend fun updateIndexLockingStatus() {
        val allWatchersAreRunning = rootWatcherStates.values.all { it is RootWatcherState.Running }
        val allRootsAreWatched = (watchedRoots - rootWatcherStates.keys).isEmpty()
        val searchIsAllowed = allWatchersAreRunning && allRootsAreWatched
        index.setSearchLockStatus(status = !searchIsAllowed)
    }
}
