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

sealed interface RootState {
    data class Initializing(val cancel: Channel<Unit>) : RootState
    data class Running(val cancel: Channel<Unit>) : RootState
    object Canceling : RootState
}

sealed interface UpdateRequest {
    val path: String

    data class RootAdded(override val path: String) : UpdateRequest
    data class RootRemoved(override val path: String) : UpdateRequest
    data class WatcherOverflown(override val path: String) : UpdateRequest
    data class RootInitialized(override val path: String) : UpdateRequest
    data class WatcherFinished(override val path: String) : UpdateRequest
}

sealed interface UserRequest {
    data class UpdateRoots(val newRoots: Set<String>) : UserRequest
    data class Search(val term: String, val ready: Channel<Unit>) : UserRequest
}

class FileIndexerImpl : FileIndexer {

    private val rootStates = mutableMapOf<String, RootState>()
    private val watchedRoots = mutableSetOf<String>()

    private val updateRequests = Channel<UpdateRequest>()

    private val watcherFinishes = Channel<String>()

    private val searchRequests = Channel<UserRequest.Search>(UNLIMITED)

    private val userRequestsChannel = Channel<UserRequest>()

    private val fileUpdateChannel = Channel<FileEvent>()

    private val watcherRunningChannel = Channel<String>()

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
                var currentRoots = setOf<String>()

                while (true) {
                    select<Unit> {
                        userRequestsChannel.onReceive { request ->
                            when (request) {
                                is UserRequest.UpdateRoots -> {
                                    val roots = request.newRoots
//                                    println("Received rootsChannel $roots")
                                    val toAdd = roots - currentRoots
                                    val toRemove = currentRoots - roots
                                    currentRoots = roots

                                    toAdd.forEach {
//                                        println("Posting ${UpdateRequest.AddRoot(it)}")
                                        updateRequests.send(UpdateRequest.RootAdded(it))
                                    }

                                    toRemove.forEach {
//                                        println("Posting ${UpdateRequest.RemoveRoot(it)}")
                                        updateRequests.send(UpdateRequest.RootRemoved(it))
                                    }
                                }
                                is UserRequest.Search -> {
                                    searchRequests.send(request)
                                }
                            }
                        }
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
                                is UpdateRequest.RootAdded -> {
                                    watchedRoots += request.path
                                }
                                is UpdateRequest.RootRemoved -> {
                                    watchedRoots -= request.path
                                    when (val rootState = rootStates[request.path]) {
                                        is RootState.Initializing -> {
                                            rootState.cancel.send(Unit)
                                            rootStates[request.path] = RootState.Canceling
                                        }
                                        is RootState.Running -> {
                                            rootState.cancel.send(Unit)
                                            rootStates[request.path] = RootState.Canceling
                                        }
                                    }
                                }
                                is UpdateRequest.RootInitialized -> {
                                    when (val rootState = rootStates[request.path]) {
                                        is RootState.Initializing -> {
                                            rootStates[request.path] = RootState.Running(rootState.cancel)
                                        }
                                    }
                                }
                                is UpdateRequest.WatcherOverflown -> {
                                    when (val rootState = rootStates[request.path]) {
                                        is RootState.Initializing -> {
                                            rootState.cancel.send(Unit)
                                            rootStates[request.path] = RootState.Canceling
                                        }
                                        is RootState.Running -> {
                                            rootState.cancel.send(Unit)
                                            rootStates[request.path] = RootState.Canceling
                                        }
                                    }
                                }
                                is UpdateRequest.WatcherFinished -> {
                                    rootStates -= request.path
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

    override suspend fun updateContentRoots(newRoots: Set<String>) {
        userRequestsChannel.send(UserRequest.UpdateRoots(newRoots))
    }

    override suspend fun searchExact(term: String): Flow<SearchResultEntry<Int>> = flow {
        val ready = Channel<Unit>(CONFLATED)
        userRequestsChannel.send(UserRequest.Search(term, ready))
        ready.receive()

        index.searchExact(term).onEach { emit(it) }.collect()
    }

    private suspend fun launchMissingWatchers(scope: CoroutineScope) {
        val updatesToRun = watchedRoots
            .filter { it !in rootStates }

        updatesToRun.forEach {
            launchWatcher(scope, it)
        }
    }

    private suspend fun launchWatcher(scope: CoroutineScope, path: String) {
        assert(rootStates[path] == null)

        val overflow = Channel<Unit>(CONFLATED)
        val cancel = Channel<Unit>(CONFLATED)
        val started = Channel<Unit>(CONFLATED)

        val watcher = RootWatcher(path, fileUpdateChannel, started, overflow, cancel)

        rootStates[path] = RootState.Initializing(cancel)

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
        val allWatchersAreRunning = rootStates.values.all { it is RootState.Running }
        val allRootsAreWatched = (watchedRoots - rootStates.keys).isEmpty()
        val searchIsAllowed = allWatchersAreRunning && allRootsAreWatched
        index.setSearchLockStatus(status = !searchIsAllowed)
    }
}

