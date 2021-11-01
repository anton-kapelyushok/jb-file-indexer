package home.pathfinder.indexing

import home.pathfinder.indexing.IndexerEvent.WatcherEvent
import home.pathfinder.indexing.RootWatcherEvent.RootWatcherLifeCycleEvent
import home.pathfinder.indexing.segmentedindex.SegmentedIndex
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import java.io.File
import java.util.*

internal sealed interface IndexerEvent {
    data class UpdateRoots(val roots: Set<WatchedRoot>) : IndexerEvent

    data class WatcherEvent(val root: WatchedRoot, val event: RootWatcherEvent) : IndexerEvent

    sealed interface Search : IndexerEvent {
        val ready: CompletableDeferred<Unit>

        data class Exact(val term: String, override val ready: CompletableDeferred<Unit>) : Search
    }
}

internal class FileIndexerImpl(
    private val tokenize: (String) -> Flow<Posting<Int>>,
) : FileIndexer {

//    private val index: HashMapIndex<Int> = HashMapIndex()
    private val index: SegmentedIndex = SegmentedIndex()

    private val rootWatcherStates = mutableMapOf<WatchedRoot, RootWatcherState>()
    private var watchedRoots = setOf<WatchedRoot>()

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

                for (indexerEvent in indexerEvents) {
                    when (indexerEvent) {
                        is IndexerEvent.UpdateRoots -> {
                            val rootRemoveRequests = rootWatcherStates.keys - indexerEvent.roots
                            rootRemoveRequests.forEach { path ->
                                rootWatcherStates.computeIfPresent(path) { _, state -> state.onInterestCeased() }
                            }

                            watchedRoots = indexerEvent.roots

                            synchronizeWatchers(workerScope)
                            updateSearchLock()
                            updateState()
                        }
                        is WatcherEvent -> {
                            when (val watcherEvent = indexerEvent.event) {
                                is RootWatcherLifeCycleEvent -> {

                                    rootWatcherStates.computeIfPresent(indexerEvent.root) { _, state ->
                                        state.onWatcherEvent(watcherEvent)
                                    }

                                    synchronizeWatchers(workerScope)
                                    updateSearchLock()
                                    updateState()
                                }
                                is RootWatcherEvent.FileUpdated -> {
                                    index.updateDocument(watcherEvent.path, readPath(watcherEvent.path))

                                }
                                is RootWatcherEvent.FileDeleted -> {
                                    index.removeDocument(watcherEvent.path)
                                }
                            }
                        }
                        is IndexerEvent.Search.Exact -> {
                            indexerEvent.ready.complete(Unit)
                        }
                    }
                }
            }
        }
    }

    private val rootsState = MutableStateFlow<Map<WatchedRoot, RootWatcherStateInfo>>(emptyMap())

    override val state = MutableStateFlow(FileIndexerStatusInfo.empty())

    override suspend fun updateContentRoots(newRoots: Set<String>, newIgnoredRoots: Set<String>) {
        val normalizedRoots = TreeSet(newRoots.map { File(it).canonicalPath })
        val normalizedIgnoredRoots = TreeSet(newIgnoredRoots.map { File(it).canonicalPath })

        val watcherMap = TreeMap<String, MutableSet<String>>()

        normalizedRoots.forEach {
            if ((normalizedRoots.higher(it) ?: "").startsWith(it)) {
                error("Cannot update roots because $it contains ${normalizedRoots.higher(it)!!}")
            }

            watcherMap[it] = mutableSetOf()
        }

        normalizedIgnoredRoots.forEach {
            if ((normalizedIgnoredRoots.higher(it) ?: "").startsWith(it)) {
                error("Cannot update roots because ignore root $it contains ${normalizedIgnoredRoots.higher(it)!!}")
            }

            val parent = watcherMap.lowerKey(it)
            if (parent == null || !it.startsWith(parent)) error("Ignored root $it is not part of any parent")

            watcherMap[parent]!!.add(it)
        }

        indexerEvents.send(IndexerEvent.UpdateRoots(watcherMap.entries.map { WatchedRoot(it.key, it.value) }.toSet()))
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

        if (rootWatcherStates.values.any { it.terminating }) return

        rootWatcherStates
            .filter { (path, _) -> path !in watchedRoots }
            .forEach { (path, _) -> rootWatcherStates.remove(path) }

        watchedRoots
            .filter { it !in rootWatcherStates }
            .forEach { launchWatcher(scope, it) }
    }

    private suspend fun launchWatcher(scope: CoroutineScope, path: WatchedRoot) {
        assert(rootWatcherStates[path] == null)

        val cancel = CompletableDeferred<Unit>()

        val watcher = RootWatcher(
            watchedRoot = path,
            cancel = cancel
        )

        rootWatcherStates[path] = RootWatcherState.Initializing(cancel)

        scope.launch {
            watcher.go(this)
            for (event in watcher.events) {
                indexerEvents.send(WatcherEvent(path, event))
            }
        }
    }

    private fun updateState() {
        rootsState.value = rootWatcherStates.map { (path, state) -> path to state.asStatus() }.toMap()
    }

    private fun readPath(path: String) = tokenize(path)

    private suspend fun updateSearchLock() {

        val allWatchersReady = rootWatcherStates.values.all { it.inConsistentState }
        val allRootsAreWatched = (watchedRoots - rootWatcherStates.keys).isEmpty()

        val searchIsAllowed = allWatchersReady && allRootsAreWatched
        index.setSearchLockStatus(status = !searchIsAllowed)
    }
}
