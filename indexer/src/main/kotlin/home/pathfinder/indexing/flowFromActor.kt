package home.pathfinder.indexing

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext

interface FlowFromActorMessage<ResultData : Any> {
    val cancelChannel: ReceiveChannel<Unit>
    val dataChannel: SendChannel<ResultData>
}

fun <MessageType : Any, ResultData : Any> flowFromActor(
    mailBox: SendChannel<MessageType>,
    createMessage: (
        receivedCancelled: ReceiveChannel<Unit>,
        dataChannel: SendChannel<ResultData>,
    ) -> MessageType
) = flow {
    val dataChannel = Channel<ResultData>()
    val cancelChannel = Channel<Unit>(CONFLATED)

    try {
        mailBox.send(createMessage(cancelChannel, dataChannel))

        for (msg in dataChannel) emit(msg)
    } finally {
        withContext(NonCancellable) {
            cancelChannel.send(Unit)
        }
    }
}

suspend fun <ResultData : Any> handleFlowFromActorMessage(
    msg: FlowFromActorMessage<ResultData>,
    fn: suspend (SendChannel<ResultData>) -> Unit
) = try {
    coroutineScope {
        val job = launch {
            try {
                fn(msg.dataChannel)
            } catch (e: Throwable) {
                msg.dataChannel.close(e)
            }
        }
        select<Unit> {
            job.onJoin {}
            msg.cancelChannel.onReceive {
                job.cancel()
                msg.dataChannel.close()
                job.join()
            }
        }
    }
} catch (e: Throwable) {
    msg.dataChannel.close(e)
    throw e
} finally {
    msg.dataChannel.close()
}

