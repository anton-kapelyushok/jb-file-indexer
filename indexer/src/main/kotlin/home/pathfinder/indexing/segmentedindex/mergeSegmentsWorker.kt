package home.pathfinder.indexing.segmentedindex

import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope

data class MergeSegmentsInput(
    val segmentsToMerge: Collection<SegmentState>
)

data class MergeSegmentsResult(
    val originalSegments: Collection<SegmentState>,
    val resultSegment: SegmentState,
)

suspend fun mergeSegmentsWorker(
    input: ReceiveChannel<MergeSegmentsInput>,
    output: SendChannel<MergeSegmentsResult>,
) = coroutineScope {
    for ((segmentsToMerge) in input) {
        val segments = segmentsToMerge.toSortedSet(
            compareBy({
                it.alivePostings
            },
                {
                    System.identityHashCode(it)
                })
        )
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
