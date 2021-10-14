package home.pathfinder.indexing

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.selects.select

// TODO: remove excessive logging
fun log(message: Any) {
    return
    val threadName = Thread.currentThread().name
    val lineNumber = Thread.currentThread().stackTrace[2].lineNumber
    println("flowFromActor.kt:$threadName:$lineNumber: $message")
}

interface FlowFromActorMessage<ResultData : Any> {
    val cancelChannel: ReceiveChannel<Unit>
    val dataChannel: SendChannel<ResultData>
}

class ActorException(e: Throwable) : RuntimeException(e)


// TODO: that is probably overcomplicated
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
        try {
            mailBox.send(createMessage(cancelChannel, dataChannel))
        } catch (e: CancellationException) {
            if (currentCoroutineContext().isActive) {
                throw ActorException(e) // because I want to distinguish job.cancel() and channel.cancel()
            } else {
                throw e
            }
        }

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
                    throw ActorException(exception)
                }
                break // channel is closed
            }
        }
    } finally {
        cancelChannel.send(Unit)
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

