package home.pathfinder.indexing

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext

// TODO: remove excessive logging
fun log(message: Any) {
    val threadName = Thread.currentThread().name
    val lineNumber = Thread.currentThread().stackTrace[2].lineNumber
    println("flowFromActor.kt:$threadName:$lineNumber: $message")
}

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
    val cancelChannel = Channel<Unit>()
    mailBox.send(createMessage(cancelChannel, dataChannel))

    while (true) {
        val next = dataChannel.receiveCatching()

        val msg = next.getOrNull()
        if (msg != null) {
            try {
                emit(msg)
            } catch (e: Exception) { // receiver exception
                log("flow emit exception $e")
                withContext(NonCancellable) {
                    log("flow cancel")
                    cancelChannel.send(Unit)
                    throw e
                }
            }
        } else {
            val exception = next.exceptionOrNull()
            if (exception != null) { // actor exception
                throw exception
            }
            break
        }
    }
}

suspend fun <ResultData : Any> handleFlowFromActorMessage(
    msg: FlowFromActorMessage<ResultData>,
    fn: suspend (SendChannel<ResultData>) -> Unit
) = supervisorScope {
    try {
        val job = launch {
            fn(msg.dataChannel)
        }
        log("before select")
        select<Unit> {
            job.onJoin {}
            msg.cancelChannel.onReceive {
                job.cancel()
                msg.dataChannel.close()
                job.join()
            }
        }
        log("after select")
    } catch (e: Exception) {
        log("close with exception")
        msg.dataChannel.close(e)
        throw e
    } finally {
        log("close finally")
        msg.dataChannel.close()
    }
}

