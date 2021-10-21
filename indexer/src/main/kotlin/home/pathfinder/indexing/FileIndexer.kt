package home.pathfinder.indexing

import home.pathfinder.indexing.IndexerEvent.WatcherEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import java.io.File
import java.util.*

internal sealed interface RootWatcherState {

    class UnexpectedWatcherStopException : RuntimeException()

    data class Initializing(override val cancel: CompletableDeferred<Unit>) : RootWatcherState, Cancelable {
        override val isTerminating: Boolean = false
        override val isReadyForSearch: Boolean = false

        override fun onWatcherEvent(event: WatcherEvent): RootWatcherState {
            return when (event) {
                is IndexerEvent.WatcherFailed -> Failing(event.exception)
                is IndexerEvent.WatcherInitialized -> Running(cancel)
                is IndexerEvent.WatcherOverflown -> onTerminate()
                is IndexerEvent.WatcherStopped -> Failing(UnexpectedWatcherStopException())
                else -> this
            }
        }

        override fun onTerminate(): RootWatcherState {
            cancel.complete(Unit)
            return Canceling
        }
    }

    data class Running(override val cancel: CompletableDeferred<Unit>) : RootWatcherState, Cancelable {
        override val isTerminating: Boolean = false
        override val isReadyForSearch = true

        override fun onWatcherEvent(event: WatcherEvent): RootWatcherState {
            return when (event) {
                is IndexerEvent.WatcherFailed -> Failing(event.exception)
                is IndexerEvent.WatcherRootDeleted -> RootRemoved
                is IndexerEvent.WatcherOverflown -> onTerminate()
                is IndexerEvent.WatcherStopped -> Failing(UnexpectedWatcherStopException())
                else -> this
            }
        }

        override fun onTerminate(): RootWatcherState {
            cancel.complete(Unit)
            return Canceling
        }
    }

    data class Failing(val error: Throwable) : RootWatcherState {
        private var rootRemoveRequested = false

        override val isTerminating = true
        override val isReadyForSearch = false

        override fun onWatcherEvent(event: WatcherEvent): RootWatcherState? {
            return when (event) {
                is IndexerEvent.WatcherFinished -> if (rootRemoveRequested) null else Failed(error)
                else -> this
            }
        }

        override fun onRootRemoveRequested(): RootWatcherState {
            rootRemoveRequested = true
            return this
        }
    }

    data class Failed(val error: Throwable) : RootWatcherState {
        override val isTerminating: Boolean = false
        override val isReadyForSearch = true

        override fun onRootRemoveRequested(): RootWatcherState? {
            return null
        }
    }

    object RootRemoved : RootWatcherState {
        override val isTerminating: Boolean = false
        override val isReadyForSearch = true

        override fun onRootRemoveRequested(): RootWatcherState? {
            return null
        }
    }

    object Canceling : RootWatcherState {
        override val isTerminating: Boolean = true
        override val isReadyForSearch = false

        override fun onWatcherEvent(event: WatcherEvent): RootWatcherState? {
            return when (event) {
                is IndexerEvent.WatcherFinished -> null
                else -> this
            }
        }
    }

    interface Cancelable {
        val cancel: CompletableDeferred<Unit>
    }

    val isTerminating: Boolean
    fun onTerminate(): RootWatcherState = this
    fun onWatcherEvent(event: WatcherEvent): RootWatcherState? = this
    fun onRootRemoveRequested(): RootWatcherState? = this
    val isReadyForSearch: Boolean
}


internal sealed interface IndexerEvent {
    data class UpdateRoots(val roots: Set<String>) : IndexerEvent
    data class WatcherOverflown(override val path: String) : WatcherEvent, IndexerEvent
    data class WatcherInitialized(override val path: String) : WatcherEvent, IndexerEvent
    data class WatcherFinished(override val path: String) : WatcherEvent, IndexerEvent
    data class WatcherFailed(override val path: String, val exception: Throwable) : WatcherEvent, IndexerEvent
    data class WatcherRootDeleted(override val path: String) : WatcherEvent, IndexerEvent
    data class WatcherStopped(override val path: String) : WatcherEvent, IndexerEvent
    data class FileUpdated(val path: String) : IndexerEvent
    data class FileRemoved(val path: String) : IndexerEvent

    sealed interface Search : IndexerEvent {
        val ready: CompletableDeferred<Unit>

        data class Exact(val term: String, override val ready: CompletableDeferred<Unit>) : Search
    }

    sealed interface WatcherEvent {
        val path: String
    }
}

internal class FileIndexerImpl(
    private val tokenize: (String) -> Flow<Posting<Int>>,
) : FileIndexer {

    private val index: HashMapIndex<Int> = HashMapIndex()

    private val rootWatcherStates = mutableMapOf<String, RootWatcherState>()
    private var watchedRoots = setOf<String>()

    private val indexerEvents = Channel<IndexerEvent>()

    override suspend fun go(scope: CoroutineScope): Job {
        return scope.launch {

            launch {
                combine(index.state, rootsState) { idx, roots ->
                    FileIndexerStatusInfo(
                        idx,
                        roots
                    )
                }.collect {
                    state.value = it
                }
            }

            launch { index.go(this) }

            launch {
                val workerScope = this

                for (request in indexerEvents) {
                    when (request) {
                        is IndexerEvent.UpdateRoots -> {
                            val rootRemoveRequests = rootWatcherStates.keys - request.roots
                            rootRemoveRequests.forEach { path ->
                                rootWatcherStates.computeIfPresent(path) { _, state -> state.onRootRemoveRequested() }
                            }

                            watchedRoots = request.roots

                            synchronizeWatchers(workerScope)
                            updateSearchLock()
                            updateState()
                        }
                        is WatcherEvent -> {
                            rootWatcherStates.computeIfPresent(request.path) { _, state ->
                                state.onWatcherEvent(request)
                            }

                            synchronizeWatchers(workerScope)
                            updateSearchLock()
                            updateState()
                        }
                        is IndexerEvent.Search.Exact -> {
                            request.ready.complete(Unit)
                        }
                        is IndexerEvent.FileUpdated -> {
                            index.updateDocument(request.path, readPath(request.path))
                        }
                        is IndexerEvent.FileRemoved -> {
                            index.removeDocument(request.path)
                        }
                    }
                }
            }
        }
    }

    private val rootsState = MutableStateFlow<Map<String, String>>(emptyMap())

    override val state = MutableStateFlow(FileIndexerStatusInfo.empty())

    override suspend fun updateContentRoots(newRoots: Set<String>) {
        val normalizedRoots = TreeSet(newRoots.map { File(it).canonicalPath })

        normalizedRoots.forEach {
            if ((normalizedRoots.higher(it) ?: "").startsWith(it)) {
                error("Cannot update roots because $it contains ${normalizedRoots.higher(it)!!}")
            }
        }

        indexerEvents.send(IndexerEvent.UpdateRoots(normalizedRoots))
    }

    override suspend fun searchExact(term: String): Flow<SearchResultEntry<Int>> = flow {
        val ready = CompletableDeferred<Unit>()
        indexerEvents.send(IndexerEvent.Search.Exact(term, ready))
        ready.await()
        index.searchExact(term).onEach { emit(it) }.collect()
    }

    private suspend fun synchronizeWatchers(scope: CoroutineScope) {
        rootWatcherStates
            .filter { (path, _) -> path !in watchedRoots }
            .forEach { (path, state) -> rootWatcherStates[path] = state.onTerminate() }

        if (rootWatcherStates.values.any { it.isTerminating }) return

        rootWatcherStates
            .filter { (path, _) -> path !in watchedRoots }
            .forEach { (path, _) -> rootWatcherStates.remove(path) }

        watchedRoots
            .filter { it !in rootWatcherStates }
            .forEach { launchWatcher(scope, it) }
    }

    private suspend fun launchWatcher(scope: CoroutineScope, path: String) {
        assert(rootWatcherStates[path] == null)

        val cancel = CompletableDeferred<Unit>()

        val watcher = RootWatcher(
            root = path,
            cancel = cancel
        )

        rootWatcherStates[path] = RootWatcherState.Initializing(cancel)

        scope.launch {
            try {
                var running = true

                val job = watcher.go(this)
                while (running) {
                    select<Unit> {
                        watcher.fileUpdated.onReceive { filePath ->
                            indexerEvents.send(IndexerEvent.FileUpdated(filePath))
                        }
                        watcher.fileRemoved.onReceive { filePath ->
                            indexerEvents.send(IndexerEvent.FileRemoved(filePath))
                        }
                        watcher.started.onReceive {
                            indexerEvents.send(IndexerEvent.WatcherInitialized(path))
                        }
                        watcher.overflow.onReceive {
                            indexerEvents.send(IndexerEvent.WatcherOverflown(path))
                        }
                        watcher.error.onReceive {
                            indexerEvents.send(IndexerEvent.WatcherFailed(path, it))
                        }
                        watcher.rootRemoved.onReceive {
                            indexerEvents.send(IndexerEvent.WatcherRootDeleted(path))
                        }
                        watcher.stoppedWatching.onReceive {
                            indexerEvents.send(IndexerEvent.WatcherStopped(path))
                        }
                        job.onJoin { running = false }
                    }
                }
            } finally {
                indexerEvents.send(IndexerEvent.WatcherFinished(path))
            }
        }
    }

    private suspend fun updateState() {
        rootsState.value = rootWatcherStates.map { (path, state) -> path to state.toString() }.toMap()
    }

    private fun readPath(path: String) = tokenize(path)

    private suspend fun updateSearchLock() {

        val allWatchersReady = rootWatcherStates.values.all { it.isReadyForSearch }
        val allRootsAreWatched = (watchedRoots - rootWatcherStates.keys).isEmpty()

        val searchIsAllowed = allWatchersReady && allRootsAreWatched
        index.setSearchLockStatus(status = !searchIsAllowed)
    }
}
