package home.pathfinder.indexing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class HashMapIndex<TermData : Any> : Index<TermData>, SearchExact<TermData> {

    private val indexState = IndexState<TermData>()
    private val indexOrchestrator = IndexOrchestrator(
        indexState = indexState,
    )

    override suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<TermData>>) {
        indexOrchestrator.updateMailbox.send(UpdateDocumentMessage(name, terms))
    }

    override suspend fun removeDocument(name: DocumentName) {
        indexOrchestrator.updateMailbox.send(UpdateDocumentMessage(name, flow {}))
    }

    override suspend fun setSearchLockStatus(status: Boolean) {
        indexOrchestrator.searchLockUpdates.send(status)
    }

    override suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<TermData>> =
        flowFromActor(indexOrchestrator.searchMailbox) { cancel, data -> SearchExactMessage(term, data, cancel) }

    override suspend fun go(scope: CoroutineScope) = indexOrchestrator.go(scope)

    override val state = indexOrchestrator.state
}

internal class IndexState<TermData : Any> {
    private val forwardIndex = mutableMapOf<DocumentName, MutableSet<Term>>()
    private val invertedIndex = mutableMapOf<Term, MutableMap<DocumentName, MutableSet<TermData>>>()

    suspend fun addTerms(documentName: DocumentName, terms: List<Posting<TermData>>) {
        terms.forEach { (term, termData) ->
            forwardIndex[documentName] =
                (forwardIndex[documentName] ?: mutableSetOf()).also { it.add(term) }
            invertedIndex[term] = (invertedIndex[term] ?: mutableMapOf()).also { termPostings ->
                termPostings[documentName] =
                    (termPostings[documentName] ?: mutableSetOf()).also { it.add(termData) }
            }
        }
    }

    suspend fun removeDocument(documentName: DocumentName) {
        (forwardIndex[documentName] ?: mutableSetOf()).forEach { term ->
            (invertedIndex[term] ?: mutableMapOf()).remove(documentName)
        }
        forwardIndex.remove(documentName)
    }

    suspend fun searchExact(term: Term, result: SendChannel<SearchResultEntry<TermData>>) {
        (invertedIndex[term] ?: mutableMapOf()).entries.forEach { (documentName, values) ->
            values.forEach { termData ->
                result.send(SearchResultEntry(documentName, term, termData))
            }
        }
    }

    fun documentsCount() = forwardIndex.size
}

internal data class UpdateDocumentMessage<TermData : Any>(
    val documentName: DocumentName,
    val flow: Flow<Posting<TermData>>
)

internal data class SearchExactMessage<TermData : Any>(
    val term: Term,
    override val dataChannel: SendChannel<SearchResultEntry<TermData>>,
    override val cancelChannel: ReceiveChannel<Unit>,
) : FlowFromActorMessage<SearchResultEntry<TermData>>

/**
 * [initial state]:
 * 1. (update request) -> schedule update; launch scheduled tasks -> move to [updating state]
 * 2. (search request) -> launch search -> move to [searching state]
 *
 * [searching state]:
 * 1. (update request) -> schedule update -> move to [searching state]
 * 2. (no scheduled tasks + search request) -> launch search -> move to [searching state]
 * 3. (search finished + finished search is not last) -> do nothing -> move to [searching state]
 * 4. (search finished + finished search is last + has scheduled tasks) -> launch scheduled tasks -> move to [updating state]
 * 5. (search finished + finished search is last + no scheduled tasks) -> do nothing -> move to [initial state]
 *
 * [updating state]:
 * 1. (update finish + has scheduled tasks) -> launch scheduled tasks -> move to [updating state]
 * 2. (update finish + no scheduled tasks) -> do nothing  -> move to [initial state]
 * 3. (update request) -> schedule update; launch scheduled tasks -> move to [updating state]
 */
internal class IndexOrchestrator<TermData : Any>(
    private val indexState: IndexState<TermData>,
    private val updateWorkersCount: Int = 1,
    private val stateUpdateBatchSize: Int = 4,
) : Actor {
    val searchMailbox = Channel<SearchExactMessage<TermData>>()
    val updateMailbox = Channel<UpdateDocumentMessage<TermData>>()
    val searchLockUpdates = Channel<Boolean>()

    private val stateUpdateMutex = Mutex()

    private var runningSearches = 0
    private var searchFinished = Channel<Unit>()

    private val runningUpdates = mutableMapOf<DocumentName, SendChannel<Unit>>()
    private val scheduledUpdates = mutableMapOf<DocumentName, UpdateDocumentMessage<TermData>>()

    private val runUpdate = Channel<Pair<UpdateDocumentMessage<TermData>, ReceiveChannel<Unit>>>()
    private val updateFinished = Channel<DocumentName>()

    private val updateFailures = Channel<Pair<DocumentName, Throwable>>()
    private val updateFailuresState = mutableMapOf<DocumentName, Throwable>()

    private var searchLocked = false

    val state = MutableStateFlow(IndexStatusInfo.empty())

    override suspend fun go(scope: CoroutineScope): Job = scope.launch {
        try {
            repeat(updateWorkersCount) { launchUpdateWorker() }

            while (true) {
                select<Unit> {
                    searchFinished.onReceive {
                        handleSearchFinished()
                    }

                    updateFailures.onReceive { (documentName, exception) ->
                        if (documentName !in scheduledUpdates) {
                            updateFailuresState[documentName] = exception
                        }
                    }

                    updateFinished.onReceive { documentName ->
                        handleUpdateFinished(documentName)
                    }

                    updateMailbox.onReceive { msg ->
                        handleUpdateRequest(msg)
                    }

                    searchLockUpdates.onReceive { v ->
                        searchLocked = v
                    }

                    if (!searchLocked && runningUpdates.isEmpty() && scheduledUpdates.isEmpty()) {
                        searchMailbox.onReceive { msg ->
                            handleSearchRequest(msg)
                        }
                    }
                }

                publishState()
            }
        } finally {
            searchMailbox.cancel()
            updateMailbox.cancel()
        }
    }

    private fun CoroutineScope.handleSearchRequest(msg: SearchExactMessage<TermData>) {
        launchSearch(msg)
    }

    private suspend fun handleUpdateRequest(msg: UpdateDocumentMessage<TermData>) {
        updateFailuresState -= msg.documentName

        runningUpdates[msg.documentName]?.send(Unit) // cancel running
        scheduledUpdates[msg.documentName] = msg
        if (runningSearches == 0) sendScheduledUpdates()
    }

    private suspend fun handleUpdateFinished(documentName: DocumentName) {
        runningUpdates.remove(documentName)
        sendScheduledUpdates()
    }

    private suspend fun handleSearchFinished() {
        runningSearches--
        if (runningSearches == 0) {
            sendScheduledUpdates()
        }
    }

    private suspend fun sendScheduledUpdates() {
        scheduledUpdates.asSequence()
            .filter { (documentName) -> documentName !in runningUpdates }
            .take(updateWorkersCount - runningUpdates.size)
            .forEach { (documentName, msg) ->
                scheduledUpdates.remove(documentName)
                sendUpdate(msg)
            }
    }

    private suspend fun sendUpdate(msg: UpdateDocumentMessage<TermData>) {
        val channel = Channel<Unit>(CONFLATED)
        runUpdate.send(msg to channel)
        runningUpdates[msg.documentName] = channel
    }

    private fun CoroutineScope.launchSearch(msg: SearchExactMessage<TermData>): Job {
        runningSearches++
        return launch {
            try {
                handleFlowFromActorMessage(msg) { result ->
                    indexState.searchExact(msg.term, result)
                }
            } catch (e: Throwable) {
                msg.dataChannel.close(e)
            } finally {
                searchFinished.send(Unit)
            }
        }
    }

    private fun CoroutineScope.launchUpdateWorker() = launch {
        suspend fun performUpdate(documentName: DocumentName, data: Flow<Posting<TermData>>) {
            try {
                val buffer = ArrayList<Posting<TermData>>(stateUpdateBatchSize)

                stateUpdateMutex.withLock { indexState.removeDocument(documentName) }

                data.collect {
                    buffer.add(it)

                    if (buffer.size == stateUpdateBatchSize) {
                        stateUpdateMutex.withLock { indexState.addTerms(documentName, buffer.toList()) }
                        buffer.clear()
                    }

                    stateUpdateMutex.withLock { indexState.addTerms(documentName, buffer) }
                }
            } catch (e: Throwable) {
                stateUpdateMutex.withLock { indexState.removeDocument(documentName) }
                throw e
            }
        }

        for ((msg, cancel) in runUpdate) {
            try {
                val job = launch {
                    try {
                        performUpdate(msg.documentName, msg.flow)
                    } catch (e: Throwable) {
                        updateFailures.send(msg.documentName to e)
                    }
                }

                select<Unit> {
                    job.onJoin {}
                    cancel.onReceive {
                        job.cancel()
                        job.join()
                    }
                }
            } finally {
                updateFinished.send(msg.documentName)
            }
        }
    }

    private fun publishState() {
        state.value = IndexStatusInfo(
            searchLocked = searchLocked,
            runningUpdates = runningUpdates.size,
            pendingUpdates = scheduledUpdates.size,
            indexedDocuments = indexState.documentsCount(),
            errors = updateFailuresState.toMap(),
        )
    }
}
