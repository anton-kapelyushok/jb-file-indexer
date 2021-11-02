package home.pathfinder.indexing.segmentedindex

import home.pathfinder.indexing.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import java.util.*

sealed interface DocumentState {
    data class Indexed(val segmentHolder: SegmentHolder) : DocumentState
    data class Scheduled(val update: DocumentMessage)
    data class Running(val cancelToken: CompletableDeferred<Unit>)
    data class Failed(val e: Throwable) : DocumentState
}

sealed interface DocumentMessage {
    val documentName: String

    data class Update(
        override val documentName: String,
        val flow: Flow<Posting<Int>>
    ) : DocumentMessage

    data class Remove(
        override val documentName: String,
    ) : DocumentMessage
}

data class SegmentHolder(var segment: SegmentState)

internal suspend fun segmentedIndexCoordinator(
    state: MutableStateFlow<IndexStatusInfo>,
    searchLockInput: ReceiveChannel<Boolean>,
    documentUpdateInput: ReceiveChannel<DocumentMessage>,
    searchInput: ReceiveChannel<SearchExactMessage<Int>>,

    createSegmentFromFileConcurrency: Int = 4,
    mergeSegmentsConcurrency: Int = 1,
    targetSegmentsCount: Int = 64,
) = coroutineScope {
    // region workers
    val createSegmentFromFileInput = Channel<CreateSegmentFromFileInput>()
    val createSegmentFromFileOutput = Channel<CreateSegmentFromFileResult>()

    val mergeSegmentsInput = Channel<MergeSegmentsInput>()
    val mergeSegmentsResult = Channel<MergeSegmentsResult>()

    try {

        repeat(createSegmentFromFileConcurrency) {
            launch {
                createSegmentFromFileWorker(
                    createSegmentFromFileInput,
                    createSegmentFromFileOutput,
                )
            }
        }

        repeat(mergeSegmentsConcurrency) {
            launch {
                mergeSegmentsWorker(mergeSegmentsInput, mergeSegmentsResult)
            }
        }
        // endregion

        // region state
        val segments = TreeSet<SegmentState>(
            // TODO
            compareBy({
                if (it.dataTermIds.isEmpty()) 0.0
                else it.alivePostings.toDouble() / it.dataTermIds.size * it.termData.size
            },
                {
                    System.identityHashCode(it)
                })
        )
        var runningMergeSegments = 0

        val indexedDocuments = mutableMapOf<String, DocumentState.Indexed>()

        val scheduledReadyDocuments = mutableMapOf<String, DocumentState.Scheduled>()
        val scheduledWaitingDocuments = mutableMapOf<String, DocumentState.Scheduled>()

        val indexingDocuments = mutableMapOf<String, DocumentState.Running>()
        val failedDocuments = mutableMapOf<String, DocumentState.Failed>()

        var expectingUpdates = false
        // endregion

        // region worker
        while (true) {
            debugLog("before receive")
            select<Unit> {
                searchLockInput.onReceive { v ->
                    debugLog("searchLockInput.onReceive $v")
                    expectingUpdates = v
                }

                documentUpdateInput.onReceive { msg ->
                    debugLog("documentUpdateInput.onReceive ${msg.documentName}")
                    val documentName = msg.documentName
                    val segment = indexedDocuments[documentName]?.segmentHolder?.segment
                    val shouldWaitForMerge = segment != null && segment !in segments


                    // TODO cancel indexing
                    if (shouldWaitForMerge) scheduledWaitingDocuments[documentName] = DocumentState.Scheduled(msg)
                    else scheduledReadyDocuments[documentName] = DocumentState.Scheduled(msg)
                }

                createSegmentFromFileOutput.onReceive { msg ->
                    debugLog("createSegmentFromFileOutput.onReceive ${msg.documentName}")
                    val documentName = msg.documentName
                    indexingDocuments.remove(documentName)
                    val isScheduled = documentName in scheduledReadyDocuments
                            || documentName in scheduledWaitingDocuments

                    if (isScheduled) return@onReceive

                    msg.data
                        .onSuccess { segment ->
                            segments += segment
                            indexedDocuments[documentName] = DocumentState.Indexed(SegmentHolder(segment))
                        }
                        .onFailure { e ->
                            failedDocuments[documentName] = DocumentState.Failed(e)
                        }
                }

                mergeSegmentsResult.onReceive { msg ->
                    debugLog("mergeSegmentsResult.onReceive ${msg.originalSegments.size}")
                    runningMergeSegments -= 1
                    segments += msg.resultSegment

                    val segmentHolder = SegmentHolder(msg.resultSegment)

                    msg.resultSegment.getAliveDocuments().forEach { docName ->
                        val removed = scheduledWaitingDocuments.remove(docName)
                        removed?.let { scheduledReadyDocuments[docName] = it }

                        indexedDocuments[docName] = DocumentState.Indexed(segmentHolder)
                    }
                }
                @Suppress("SimplifyBooleanWithConstants")
                val indexIsOnline = true
                        && !expectingUpdates
                        && indexingDocuments.isEmpty()
                        && scheduledReadyDocuments.isEmpty()
                        && scheduledWaitingDocuments.isEmpty()
                        && runningMergeSegments == 0

                if (indexIsOnline) {
                    searchInput.onReceive { msg ->
                        debugLog("searchInput.onReceive $msg")
                        handleSearchRequest(msg, segments.toSet())
                    }
                }
            }

            run { // run updates
                scheduledReadyDocuments.entries
                    // TODO filter out indexing
                    .take(createSegmentFromFileConcurrency - indexingDocuments.size)
                    .forEach { (docName, msg) ->
                        scheduledReadyDocuments.remove(docName)

                        val segmentHolder = indexedDocuments[docName]?.segmentHolder
                        if (segmentHolder != null) {
                            indexedDocuments.remove(docName)
                            segments -= segmentHolder.segment
                            val newSegment = segmentHolder.segment.deleteDocument(docName)
                            segments += newSegment

                            segmentHolder.segment = newSegment
                        }

                        if (msg.update is DocumentMessage.Update) {
                            val cancelToken = CompletableDeferred<Unit>()
                            debugLog("before createSegmentFromFileInput.send")
                            createSegmentFromFileInput.send(
                                CreateSegmentFromFileInput(docName, msg.update.flow, cancelToken)
                            )
                            indexingDocuments[docName] = DocumentState.Running(cancelToken)
                        }
                    }
            }

            run { // run merges
                while (runningMergeSegments < mergeSegmentsConcurrency
                    && segments.size > targetSegmentsCount
//                    && indexingDocuments.isEmpty()
                ) {
                    runningMergeSegments++
                    val segmentsToMerge =
                        segments.take(
                            maxOf(minOf(segments.size - targetSegmentsCount + 1, 64, segments.size / 16), 2)
                        )
                    segments -= segmentsToMerge
                    debugLog("before mergeSegmentsInput.send")
                    mergeSegmentsInput.send(MergeSegmentsInput(segmentsToMerge))
                    segmentsToMerge.forEach { segment ->
                        segment.getAliveDocuments().forEach { docName ->
                            val removed = scheduledReadyDocuments.remove(docName)
                            if (removed != null) {
                                scheduledWaitingDocuments[docName] = removed
                            }
                        }
                    }
                }
            }

            state.value = IndexStatusInfo(
                searchLocked = expectingUpdates,
                runningUpdates = indexingDocuments.size,
                pendingUpdates = scheduledReadyDocuments.size + scheduledWaitingDocuments.size,
                indexedDocuments = indexedDocuments.size,
                errors = failedDocuments.mapValues { (_, v) -> v.e },
                segments = segments.size
            )
        }
        // endregion
    } finally {
        createSegmentFromFileInput.close()
        createSegmentFromFileOutput.close()
        mergeSegmentsInput.close()
        mergeSegmentsResult.close()
    }
}

private fun CoroutineScope.handleSearchRequest(msg: SearchExactMessage<Int>, segments: Set<SegmentState>) {
    launch {
        try {
            handleFlowFromActorMessage(msg) { result ->
                segments.forEach { segment ->
                    segment.find(msg.term).forEach {
                        result.send(it)
                    }
                }
            }
        } catch (e: Throwable) {
            msg.dataChannel.close(e)
        }
    }
}
