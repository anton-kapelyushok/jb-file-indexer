package home.pathfinder.indexing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class HashMapIndex<TermData : Any> : Index<TermData>, SearchExact<TermData> {

    private val indexState = IndexState<TermData>()
    private val indexOrchestrator = IndexOrchestrator(
        indexState = indexState,
        updateWorkersCount = 2,
    )

    override suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<TermData>>) {
        indexOrchestrator.updateMailbox.send(UpdateDocumentMessage(name, terms))
    }

    override suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<TermData>> =
        flowFromActor(indexOrchestrator.searchMailbox) { cancel, data -> SearchExactMessage(term, data, cancel) }

    fun go(scope: CoroutineScope) = indexOrchestrator.go(scope)
}

class IndexState<TermData : Any> {
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
        result.close()
    }
}

data class UpdateDocumentMessage<TermData : Any>(
    val documentName: DocumentName,
    val flow: Flow<Posting<TermData>>
)

data class SearchExactMessage<TermData : Any>(
    val term: Term,
    override val dataChannel: SendChannel<SearchResultEntry<TermData>>,
    override val cancelChannel: ReceiveChannel<Unit>,
) : FlowFromActorMessage<SearchResultEntry<TermData>>

/**
 * [initial state]:
 * 1. (update request) -> launch update -> move to [updating state]
 * 2. (search request) -> launch search -> move to [searching state]
 *
 * [searching state]:
 * 1. (update request) -> schedule update -> move to [searching state]
 * 2. (scheduled updates is empty + search request) -> launch search -> move to [searching state]
 * 3. (search finished + finished search is not last) -> do nothing -> move to [searching state]
 * 4. (search finished + finished search is last + scheduled updates empty) -> launch updates -> move to [updating state]
 * 5. (search finished + finished search is last + scheduled updates empty) -> do nothing -> move to [initial state]
 *
 * [updating state]:
 * 1. (update finish + scheduled is empty + running is empty) -> do nothing  -> move to [initial state]
 * 2. (update finish + scheduled is empty + running is not empty) -> do nothing  -> move to [updating state]
 * 3. (update finish + scheduled is not empty) -> launch scheduled -> move to [updating state]
 * 4. (update request + running is not empty) -> cancel running + schedule update -> move to [updating state]
 * 5. (update request + running is empty) -> launch update -> move to [updating state]
 */
class IndexOrchestrator<TermData : Any>(
    private val indexState: IndexState<TermData>,
    private val updateWorkersCount: Int = 2,
    private val stateUpdateBatchSize: Int = 1000,
) {
    val searchMailbox = Channel<SearchExactMessage<TermData>>()
    val updateMailbox = Channel<UpdateDocumentMessage<TermData>>()

    private val stateUpdateMutex = Mutex()

    private var runningSearches = 0
    private var searchFinished = Channel<Unit>()

    private val runningUpdates = mutableMapOf<DocumentName, SendChannel<Unit>>()
    private val scheduledUpdates = mutableMapOf<DocumentName, UpdateDocumentMessage<TermData>>()

    // unlimited because I want to cancel running outdated tasks
    private val runUpdate = Channel<Pair<UpdateDocumentMessage<TermData>, ReceiveChannel<Unit>>>(UNLIMITED)
    private val updateFinished = Channel<DocumentName>()

    fun go(scope: CoroutineScope): Job = scope.launch {
        try {
            repeat(updateWorkersCount) { launchUpdateWorker() }

            while (true) {
                select<Unit> {
                    searchFinished.onReceive {
                        println("received searchFinished")
                        handleSearchFinished()
                    }

                    updateFinished.onReceive { documentName ->
                        println("received update finished on$documentName")
                        handleUpdateFinished(documentName)
                    }

                    if (runningSearches == 0) {
                        this@IndexOrchestrator.updateMailbox.onReceive { msg ->
                            println("received $msg")
                            handleUpdateRequest(msg)
                        }
                    }
                    if (runningUpdates.isEmpty() && scheduledUpdates.isEmpty()) {
                        this@IndexOrchestrator.searchMailbox.onReceive { msg ->
                            println("received $msg")
                            handleSearchRequest(msg)
                        }
                    }
                }
            }
        } finally {
            this@IndexOrchestrator.searchMailbox.cancel()
            this@IndexOrchestrator.updateMailbox.cancel()
        }

    }

    private fun CoroutineScope.handleSearchRequest(msg: SearchExactMessage<TermData>) {
        launchSearch(msg)
    }

    private suspend fun handleUpdateRequest(msg: UpdateDocumentMessage<TermData>) {
        val runningUpdate = runningUpdates[msg.documentName]
        if (runningUpdate != null) {
            runningUpdate.send(Unit) // cancel current update
            scheduledUpdates[msg.documentName] = msg // schedule latest
        } else {
            sendUpdate(msg)
        }
    }

    private suspend fun handleUpdateFinished(documentName: DocumentName) {
        runningUpdates.remove(documentName)

        if (documentName in scheduledUpdates) {
            val msg = scheduledUpdates[documentName]!!
            scheduledUpdates.remove(documentName)
            sendUpdate(msg)
        }
    }

    private suspend fun handleSearchFinished() {
        runningSearches--
        if (runningSearches == 0) {
            scheduledUpdates.forEach { (_, msg) ->
                sendUpdate(msg)
            }
        }
    }

    private suspend fun sendUpdate(msg: UpdateDocumentMessage<TermData>) {
        val channel = Channel<Unit>(CONFLATED)
        runUpdate.send(msg to channel)
        runningUpdates[msg.documentName] = channel
    }

    private fun CoroutineScope.launchSearch(msg: SearchExactMessage<TermData>) = launch {
        runningSearches++

        try {
            handleFlowFromActorMessage(msg) { result ->
                indexState.searchExact(msg.term, result)
            }
        } finally {
            searchFinished.send(Unit)
        }
    }


    private fun CoroutineScope.launchUpdateWorker() = launch {
        suspend fun performUpdate(documentName: DocumentName, data: Flow<Posting<TermData>>) {
            val buffer = mutableListOf<Posting<TermData>>()

            stateUpdateMutex.withLock { indexState.removeDocument(documentName) }

            data.collect {
                buffer.add(it)

                if (buffer.size == stateUpdateBatchSize) {
                    stateUpdateMutex.withLock { indexState.addTerms(documentName, buffer.toList()) }
                    buffer.clear()
                }

                stateUpdateMutex.withLock { indexState.addTerms(documentName, buffer) }
            }
        }

        for ((msg, cancel) in runUpdate) {
            try {
                supervisorScope {
                    val job = launch {
                        performUpdate(msg.documentName, msg.flow)
                    }

                    select<Unit> {
                        job.onJoin {}
                        cancel.onReceive {
                            job.cancel()
                            job.join()
                        }
                    }
                }
            } finally {
                updateFinished.send(msg.documentName)
            }
        }
    }
}