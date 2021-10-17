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
    data class Initializing(val cancel: Channel<Unit>) : RootWatcherState
    data class Running(val cancel: Channel<Unit>) : RootWatcherState
    object Canceling : RootWatcherState
}

sealed interface UpdateRequest {
    data class UpdateRoots(val roots: Set<String>) : UpdateRequest
    data class WatcherOverflown(val path: String) : UpdateRequest
    data class RootInitialized(val path: String) : UpdateRequest
    data class WatcherFinished(val path: String) : UpdateRequest
}

sealed interface SearchRequest {
    val ready: Channel<Unit>

    data class SearchExact(val term: String, override val ready: Channel<Unit>) : SearchRequest
}

class FileIndexerImpl : FileIndexer {

    private val rootWatcherStates = mutableMapOf<String, RootWatcherState>()
    private var watchedRoots = mutableSetOf<String>()
    private val updateRequests = Channel<UpdateRequest>()
    private val searchRequests = Channel<SearchRequest>(UNLIMITED)
    private val fileUpdateChannel = Channel<FileEvent>(UNLIMITED)
    private val index = HashMapIndex<Int>()

    override suspend fun go(scope: CoroutineScope) {
        scope.launch {
            launch { index.go(this) }

            launch {
                for (msg in fileUpdateChannel) {
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
                        updateRequests.onReceive { request ->
                            println("Received updateRequests $request")
                            when (request) {
                                is UpdateRequest.UpdateRoots -> {
                                    val watchersToRemove = watchedRoots - request.roots
                                    watchedRoots = request.roots.toMutableSet()
                                    watchersToRemove.forEach { cancelWatcher(it) }
                                }
                                is UpdateRequest.RootInitialized -> {
                                    when (val rootState = rootWatcherStates[request.path]) {
                                        is RootWatcherState.Initializing -> {
                                            rootWatcherStates[request.path] = RootWatcherState.Running(rootState.cancel)
                                        }
                                    }
                                }
                                is UpdateRequest.WatcherOverflown -> {
                                    cancelWatcher(request.path)
                                }
                                is UpdateRequest.WatcherFinished -> {
                                    rootWatcherStates -= request.path
                                }
                            }

                            launchMissingWatchers(workerScope)
                            updateIndexLockingStatus()
                        }

                        searchRequests.onReceive { request ->
                            request.ready.send(Unit)
                        }
                    }
                }
            }
        }
    }

    private suspend fun cancelWatcher(root: String) {
        when (val rootState = rootWatcherStates[root]) {
            is RootWatcherState.Initializing -> {
                rootState.cancel.send(Unit)
                rootWatcherStates[root] = RootWatcherState.Canceling
            }
            is RootWatcherState.Running -> {
                rootState.cancel.send(Unit)
                rootWatcherStates[root] = RootWatcherState.Canceling
            }
        }
    }

    override suspend fun updateContentRoots(newRoots: Set<String>) {
        updateRequests.send(UpdateRequest.UpdateRoots(newRoots))
    }

    override suspend fun searchExact(term: String): Flow<SearchResultEntry<Int>> = flow {
        val ready = Channel<Unit>(CONFLATED)
        searchRequests.send(SearchRequest.SearchExact(term, ready))
        ready.receive()
        index.searchExact(term).onEach { emit(it) }.collect()
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

        val watcher = RootWatcher(path, fileUpdateChannel, started, overflow, cancel)

        rootWatcherStates[path] = RootWatcherState.Initializing(cancel)

        scope.launch {
            try {
                var running = true
                val job = watcher.go(this)
                while (running) {
                    select<Unit> {
                        started.onReceive {
                            updateRequests.send(UpdateRequest.RootInitialized(path))
                        }
                        overflow.onReceive {
                            updateRequests.send(UpdateRequest.WatcherOverflown(path))
                        }
                        job.onJoin { running = false }
                    }
                }
            } finally {
                updateRequests.send(UpdateRequest.WatcherFinished(path))
            }
        }
    }

    override val stateFlow: StateFlow<FileIndexerState> get() = null!!

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

