package home.pathfinder.indexing.segmentedindex

import home.pathfinder.indexing.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select

// WIP
internal class SegmentedIndex : Index<Int>, SearchExact<Int> {

    private val indexOrchestrator = IndexOrchestrator()

    override suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<Int>>) {
        indexOrchestrator.updateMailbox.send(InboundDocumentMessage.Update(name, terms))
    }

    override suspend fun removeDocument(name: DocumentName) {
        indexOrchestrator.updateMailbox.send(InboundDocumentMessage.Remove(name))
    }

    override suspend fun setSearchLockStatus(status: Boolean) {
        indexOrchestrator.searchLockUpdates.send(status)
    }

    override suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<Int>> =
        flowFromActor(indexOrchestrator.searchMailbox) { cancel, data -> SearchExactMessage(term, data, cancel) }

    override suspend fun go(scope: CoroutineScope) = indexOrchestrator.go(scope)

    override val state = indexOrchestrator.state
}

internal sealed interface DocumentMessage {
    val documentName: String

    data class Update(
        val oldSegment: SegmentState?,
        override val documentName: String,
        val flow: Flow<Posting<Int>>
    ) : DocumentMessage

    data class Remove(
        val oldSegment: SegmentState?,
        override val documentName: String,
    ) : DocumentMessage
}

internal sealed interface InboundDocumentMessage {
    val documentName: String

    data class Update(
        override val documentName: String,
        val flow: Flow<Posting<Int>>
    ) : InboundDocumentMessage

    data class Remove(
        override val documentName: String,
    ) : InboundDocumentMessage
}

internal data class PerformUpdateMessage(
    val toDelete: List<SegmentState>,
    val toAdd: List<SegmentState>,
)

class IndexOrchestrator(
    private val updateWorkersCount: Int = 1,
) : Actor {
    internal val searchMailbox = Channel<SearchExactMessage<Int>>()
    internal val updateMailbox = Channel<InboundDocumentMessage>()
    internal val searchLockUpdates = Channel<Boolean>()

    private val updateWorkerQueue = Channel<Pair<DocumentMessage, ReceiveChannel<Unit>>>()
    private val updateFinished = Channel<String>()

    private val runningUpdates = mutableMapOf<String, SendChannel<Unit>>()

    private val updateFailures = Channel<Pair<String, Throwable>>()
    private val updateFailuresState = mutableMapOf<String, Throwable>()

    private val scheduledUpdates = mutableMapOf<String, InboundDocumentMessage>()

    private val performUpdateQueue = Channel<PerformUpdateMessage>()

    private var segments = setOf<SegmentState>()
    private val documentToSegment = mutableMapOf<String, SegmentState>()

    override suspend fun go(scope: CoroutineScope): Job = scope.launch {
        try {
            repeat(updateWorkersCount) { launchUpdateWorker() }

            while (true) {
                select<Unit> {
                    performUpdateQueue.onReceive { msg ->
                        segments = segments - msg.toDelete + msg.toAdd
                        msg.toDelete.forEach { segment -> segment.docNames.forEach { documentToSegment -= it } }
                        msg.toAdd.forEach { segment -> segment.docNames.forEach { documentToSegment[it] = segment } }
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
                            handleSearchRequest(msg, segments)
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

    private fun CoroutineScope.handleSearchRequest(msg: SearchExactMessage<Int>, segments: Set<SegmentState>) {
        launch {
            try {
                handleFlowFromActorMessage(msg) { result ->
                    segments.forEach { segment ->
                        findInSegment(segment, msg.term).forEach {
                            result.send(it)
                        }
                    }
                }
            } catch (e: Throwable) {
                msg.dataChannel.close(e)
            }
        }
    }


    private suspend fun handleUpdateFinished(documentName: String) {
        runningUpdates.remove(documentName)
        sendScheduledUpdates()
    }

    private suspend fun sendScheduledUpdates() {
        scheduledUpdates.asSequence()
            .filter { /* TODO: check segment is not being updated */ true }
            .filter { (documentName) -> documentName !in runningUpdates }
            .take(updateWorkersCount - runningUpdates.size)
            .forEach { (documentName, msg) ->
                scheduledUpdates.remove(documentName)
                val segment = documentToSegment[documentName]
                val newMsg = when (msg) {
                    is InboundDocumentMessage.Update -> DocumentMessage.Update(segment, msg.documentName, msg.flow)
                    is InboundDocumentMessage.Remove -> DocumentMessage.Remove(segment, msg.documentName)
                }
                sendUpdate(newMsg)
            }
    }

    private suspend fun sendUpdate(msg: DocumentMessage) {
        val channel = Channel<Unit>(Channel.CONFLATED)
        updateWorkerQueue.send(msg to channel)
        runningUpdates[msg.documentName] = channel
    }

    private suspend fun handleUpdateRequest(msg: InboundDocumentMessage) {
        updateFailuresState -= msg.documentName

        runningUpdates[msg.documentName]?.send(Unit) // cancel running
        scheduledUpdates[msg.documentName] = msg
        sendScheduledUpdates()
    }


    private fun CoroutineScope.launchUpdateWorker() = launch {
        for ((msg, cancel) in updateWorkerQueue) {
            try {
                val job = launch {
                    try {
                        when (msg) {
                            is DocumentMessage.Remove -> {
                                if (msg.oldSegment != null) {
                                    performUpdateQueue.send(
                                        PerformUpdateMessage(
                                            toDelete = listOf(msg.oldSegment),
                                            toAdd = listOf(
                                                deleteDocument(msg.oldSegment, msg.documentName)
                                            )
                                        )
                                    )
                                }
                            }
                            is DocumentMessage.Update -> {
                                performUpdateQueue.send(
                                    PerformUpdateMessage(
                                        toDelete = listOfNotNull(msg.oldSegment),
                                        toAdd = listOf(createSegment(msg.documentName, msg.flow.toList()))
                                    )
                                )
                            }
                        }
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
            indexedDocuments = 0, // TODO
            errors = updateFailuresState.toMap(),
        )
    }

    private var searchLocked = false

    val state = MutableStateFlow(IndexStatusInfo.empty())
}


@Suppress("ArrayInDataClass")
data class SegmentState(
    val docNames: Array<String>,
    val docStates: BooleanArray,
    val terms: Array<String>,

    val termArray: IntArray,
    val docArray: IntArray,
    val termDataArray: IntArray,

    val postingsPerDocument: IntArray,
    val alivePostings: Int,
) {
    override fun hashCode(): Int {
        return super.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return this === other
    }
}

fun createSegment(documentName: String, documentData: List<Posting<Int>>): SegmentState {
    val documentNames = arrayOf(documentName)
    val documentStates = booleanArrayOf(true)
    val terms = documentData.map { it.term }.sorted().distinct().toTypedArray()

    val termArray = IntArray(documentData.size)
    val docArray = IntArray(documentData.size)
    val termDataArray = IntArray(documentData.size)

    val postingsPerDocument = intArrayOf(documentData.size)
    val alivePostings = documentData.size

    documentData
        .sortedBy { it.term }
        .forEachIndexed { idx, (term, termData) ->
            val termV = terms.binarySearch(term)
            termArray[idx] = termV
            docArray[idx] = 0
            termDataArray[idx] = termData
        }

    return SegmentState(
        docNames = documentNames,
        docStates = documentStates,
        terms = terms,
        termArray = termArray,
        docArray = docArray,
        termDataArray = termDataArray,
        postingsPerDocument = postingsPerDocument,
        alivePostings = alivePostings
    )
}

fun deleteDocument(segment: SegmentState, documentName: String): SegmentState {
    val docIdx = segment.docNames.binarySearch(documentName)
    return segment.copy(
        docStates = segment.docStates.copyOf().also { it[docIdx] = false },
        alivePostings = segment.alivePostings - segment.postingsPerDocument[docIdx]
    )
}

fun mergeSegments(lSegment: SegmentState, rSegment: SegmentState): SegmentState {
    val aliveDocIndicesLeft = lSegment.docNames.indices.filter { lSegment.docStates[it] }
    val aliveDocIndicesRight = rSegment.docNames.indices.filter { rSegment.docStates[it] }
    val newDocNames =
        (aliveDocIndicesLeft.map { lSegment.docNames[it] } + aliveDocIndicesRight.map { rSegment.docNames[it] })
            .distinct()
            .sorted()
            .toTypedArray()

    val newDocStates = BooleanArray(newDocNames.size) { true }

    val newAlivePostings = lSegment.alivePostings + rSegment.alivePostings

    val newTermArray = IntArray(newAlivePostings)
    val newDocArray = IntArray(newAlivePostings)
    val newTermDataArray = IntArray(newAlivePostings)

    val newTerms = mutableListOf<String>()

    val newPostingsPerDocument = IntArray(newDocNames.size)

    run {
        var r = 0
        var l = 0

        var last = 0

        fun addFrom(segment: SegmentState, i: Int) {
            val term = segment.terms[segment.termArray[i]]
            if (newTerms.isEmpty() || term != newTerms.last()) {
                newTerms.add(term)
            }
            val termId = newTerms.size - 1
            newTermArray[last] = termId

            val docName = segment.docNames[segment.docArray[i]]
            val docId = newDocNames.binarySearch(docName)

            newDocArray[last] = docId

            newPostingsPerDocument[docId]++

            newTermDataArray[last] = segment.termDataArray[i]

            last++
        }

        while (l != lSegment.termArray.size || r != rSegment.termArray.size) {
            when {
                l in lSegment.termArray.indices && !lSegment.docStates[lSegment.docArray[l]] -> {
                    l++
                }
                r in rSegment.termArray.indices && !rSegment.docStates[rSegment.docArray[r]] -> {
                    r++
                }
                l !in lSegment.termArray.indices -> {
                    addFrom(rSegment, r)
                    r++
                }
                r !in rSegment.termArray.indices -> {
                    addFrom(lSegment, l)
                    l++
                }
                else -> {
                    val lTerm = lSegment.terms[lSegment.termArray[l]]
                    val rTerm = rSegment.terms[rSegment.termArray[r]]
                    if (lTerm < rTerm) {
                        addFrom(lSegment, l)
                        l++
                    } else {
                        addFrom(rSegment, r)
                        r++
                    }
                }
            }
        }

        return SegmentState(
            docNames = newDocNames,
            docStates = newDocStates,
            terms = newTerms.toTypedArray(),
            termArray = newTermArray,
            docArray = newDocArray,
            termDataArray = newTermDataArray,
            postingsPerDocument = newPostingsPerDocument,
            alivePostings = newAlivePostings
        )
    }
}

fun findInSegment(segment: SegmentState, term: String): List<SearchResultEntry<Int>> {
    val termId = segment.terms.binarySearch(term)
    if (termId < 0) return listOf()

    var i = segment.termArray.binarySearch(termId)
    while (i - 1 in segment.termArray.indices && segment.termArray[i - 1] == termId) i--

    val result = mutableListOf<SearchResultEntry<Int>>()

    while (i < segment.termArray.size) {
        if (!segment.docStates[segment.docArray[i]]) i++
        if (segment.termArray[i] != termId) break
        result.add(
            SearchResultEntry(
                documentName = segment.docNames[segment.docArray[i]],
                term = segment.terms[segment.termArray[i]],
                termData = segment.termDataArray[i]
            )
        )
        i++
    }

    return result
}

