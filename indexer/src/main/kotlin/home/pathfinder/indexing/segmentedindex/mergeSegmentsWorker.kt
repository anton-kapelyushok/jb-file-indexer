package home.pathfinder.indexing.segmentedindex

import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope

internal data class MergeSegmentsInput(
    val segmentsToMerge: Collection<SegmentState>
)

internal data class MergeSegmentsResult(
    val originalSegments: Collection<SegmentState>,
    val resultSegment: SegmentState,
)

internal suspend fun mergeSegmentsWorker(
    input: ReceiveChannel<MergeSegmentsInput>,
    output: SendChannel<MergeSegmentsResult>,
) = coroutineScope {
    for ((segmentsToMerge) in input) {
        val segments = segmentsToMerge.toSortedSet(compareBy({ it.alivePostings }, { it.id }))
        while (segments.size > 1) {
            val (segment1, segment2) = segments.take(2)
            val newSegment = mergeSegments(segment1, segment2)
            segments += newSegment
            segments -= segment1
            segments -= segment2
        }

        output.send(MergeSegmentsResult(originalSegments = segmentsToMerge, resultSegment = segments.first()))
    }
}
