package home.pathfinder.indexing.segmentedindex

import home.pathfinder.indexing.Posting
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.selects.select

data class CreateSegmentFromFileInput(
    val path: String,
    val data: Flow<Posting<Int>>,
    val cancelToken: CompletableDeferred<Unit>
)

data class CreateSegmentFromFileResult(
    val documentName: String,
    val data: Result<SegmentState>,
)

suspend fun createSegmentFromFileWorker(
    input: ReceiveChannel<CreateSegmentFromFileInput>,
    output: SendChannel<CreateSegmentFromFileResult>,
) = coroutineScope {
    for (msg in input) {
        if (msg.cancelToken.isCompleted) continue
        val dataDeferred = async {
            runCatching { msg.data.toList() }
                .map { createSegment(msg.path, it) }
        }
        select<Unit> {
            msg.cancelToken.onJoin {
                dataDeferred.cancel()
                dataDeferred.join()
                output.send(CreateSegmentFromFileResult(msg.path, Result.failure(CancellationException())))
            }
            dataDeferred.onAwait { result ->
                output.send(CreateSegmentFromFileResult(msg.path, result))
            }
        }
    }
}

