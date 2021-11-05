package home.pathfinder.indexing

import home.pathfinder.indexing.IndexerEvent.WatcherEvent
import home.pathfinder.indexing.RootWatcherEvent.RootWatcherLifeCycleEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import java.io.File
import java.nio.file.Paths
import java.util.*
import kotlin.io.path.isRegularFile

internal sealed interface IndexerEvent {
    data class UpdateRoots(val roots: Set<WatchedRoot>) : IndexerEvent

    data class WatcherEvent(val root: WatchedRoot, val event: RootWatcherEvent) : IndexerEvent

    sealed interface Search : IndexerEvent {
        val ready: CompletableDeferred<Unit>

        data class Exact(val term: String, override val ready: CompletableDeferred<Unit>) : Search
    }
}

internal class FileIndexerImpl(
    private val index: Index<Int>,
    private val tokenize: (String) -> Flow<Posting<Int>>,
    private val additionalProperties: Map<String, Any>,
) : FileIndexer {

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
        val normalizedRoots = PathTree(newRoots.map { File(it).canonicalPath })
        val normalizedIgnoredRoots = PathTree(newIgnoredRoots.map { File(it).canonicalPath })

        normalizedRoots.forEach { // check no intersections
            val parent = normalizedRoots.parentOf(it)
            if (parent != null) {
                error("Cannot update roots because $parent contains $it")
            }
        }

        val (fileRootPaths, directoryRootPaths) = normalizedRoots.map { Paths.get(it) }.partition { it.isRegularFile() }
        val fileRoots = fileRootPaths.map { it.toString() }
        val fileParents = fileRootPaths.map { it.parent.toString() }
        val directoryRoots = directoryRootPaths.map { it.toString() }

        val minimalRoots = minimalRootsTree((directoryRoots + fileParents))

        data class WatcherBuilder(
            val ignoredRoots: MutableSet<String> = mutableSetOf(),
            val actualRoots: MutableSet<String> = mutableSetOf(),
        )

        val watcherMap = TreeMap<String, WatcherBuilder>()
        minimalRoots.forEach { watcherMap[it] = WatcherBuilder() }

        normalizedIgnoredRoots.forEach {
            if (normalizedIgnoredRoots.containsParentOf(it)) {
                error("Cannot update roots because ignore root $it contains ${normalizedIgnoredRoots.parentOf(it)!!}")
            }

            normalizedRoots.parentOf(it) ?: error("Ignored root $it is not part of any parent")

            watcherMap[minimalRoots.parentOf(it)]!!.ignoredRoots += it
        }

        (directoryRoots + fileRoots).forEach {
            watcherMap[minimalRoots.pathOrItsParent(it)]!!.actualRoots += it
        }

        indexerEvents.send(IndexerEvent.UpdateRoots(watcherMap.entries
            .map {
                WatchedRoot(
                    root = it.key,
                    ignoredRoots = it.value.ignoredRoots,
                    actualRoots = it.value.actualRoots
                )
            }
            .toSet()))
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

        val watcher = RootWatcher(path, rwInitialEmitFromFileHasherHackDisabled())

        rootWatcherStates[path] = RootWatcherState.Initializing { watcher.cancel() }

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

    private fun rwInitialEmitFromFileHasherHackDisabled(): Boolean =
        additionalProperties[rwInitialEmitFromFileHasherHackDisabled] == true
}
