package home.pathfinder.indexing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
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

    data class AddRoot(override val path: String) : UpdateRequest
    data class RemoveRoot(override val path: String) : UpdateRequest
}

sealed interface UserRequest {
    data class UpdateRoots(val newRoots: Set<String>) : UserRequest
    data class Search(val term: String, val ready: Channel<Unit>) : UserRequest
}

class FileIndexerImpl : FileIndexer {

    private val runningJobs = mutableMapOf<String, RootState>()
    private val updateRequests = Channel<UpdateRequest>()
    private val watcherFinishes = Channel<String>()

    private val searchRequests = Channel<UserRequest.Search>(UNLIMITED)

    private val scheduledUpdates = mutableSetOf<String>()

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
                        is FileEvent.FileRemoved -> index.updateDocument(msg.path, flow {})
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
                                        updateRequests.send(UpdateRequest.AddRoot(it))
                                    }

                                    toRemove.forEach {
//                                        println("Posting ${UpdateRequest.RemoveRoot(it)}")
                                        updateRequests.send(UpdateRequest.RemoveRoot(it))
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
                        watcherFinishes.onReceive { path ->
//                            println("Received updateFinishes $path")
                            runningJobs.remove(path)
                            launchScheduledUpdates(workerScope)

                            updateIndexLockingStatus()
                        }

                        updateRequests.onReceive { request ->
//                            println("Received updateRequests $request")
                            when (request) {
                                is UpdateRequest.AddRoot -> {
                                    when (val runningUpdate = runningJobs[request.path]) {
                                        null, is RootState.Canceling -> scheduledUpdates += request.path
                                        is RootState.Initializing, is RootState.Running -> Unit
                                    }
                                }
                                is UpdateRequest.RemoveRoot -> {
                                    when (val runningUpdate = runningJobs[request.path]) {
                                        null, is RootState.Canceling -> scheduledUpdates -= request.path
                                        is RootState.Initializing -> {
                                            runningUpdate.cancel.send(Unit)
                                            runningJobs[request.path] = RootState.Canceling
                                        }
                                        is RootState.Running -> {
                                            runningUpdate.cancel.send(Unit)
                                            runningJobs[request.path] = RootState.Canceling
                                        }
                                    }
                                }
                            }

                            launchScheduledUpdates(workerScope)
                            updateIndexLockingStatus()
                        }

                        watcherRunningChannel.onReceive { path ->
//                            println("Received watcherRunningChannel")

                            val runningJob = runningJobs[path] ?: return@onReceive
                            if (runningJob !is RootState.Initializing) return@onReceive
                            runningJobs[path] = RootState.Running(runningJob.cancel)
                            updateIndexLockingStatus()
                        }

                        searchRequests.onReceive { request ->
//                            println("Received searchRequests $request")
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
        val ready = Channel<Unit>()
        userRequestsChannel.send(UserRequest.Search(term, ready))
        ready.receive()

        index.searchExact(term).onEach { emit(it) }.collect()
    }

    private suspend fun launchScheduledUpdates(scope: CoroutineScope) {
        val updatesToRun = scheduledUpdates
            .filter { it !in runningJobs }

        println("updatesToRun $updatesToRun")

        updatesToRun.forEach {
            launchWatcher(scope, it)
        }

        scheduledUpdates.removeAll(updatesToRun)
    }

    private suspend fun launchWatcher(scope: CoroutineScope, path: String) {
        assert(runningJobs[path] == null)

        val watcher = RootWatcher(path, fileUpdateChannel)
        val cancel = Channel<Unit>()
        val started = Channel<Unit>()

        runningJobs[path] = RootState.Initializing(cancel)

        scope.launch {
            try {
                var running = true
//                println("launchWatcher launch $path")
                val job = watcher.go(this, started, cancel)
                while (running) {
                    select<Unit> {
                        started.onReceive {
                            watcherRunningChannel.send(path)
                        }
                        job.onJoin { running = false }
                    }
                }
            } finally {
                watcherFinishes.send(path)
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
        val hasRunningBatches = runningJobs.values.any { it !is RootState.Running }
        val hasScheduledBatches = scheduledUpdates.size > 0
        val searchLocked = hasRunningBatches || hasScheduledBatches

        index.setSearchLockStatus(searchLocked)
    }
}

