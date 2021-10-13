package home.pathfinder.indexing

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Test

internal class FlowFromActorKtTest {

    data class Message(
        override val cancelChannel: ReceiveChannel<Unit>,
        override val dataChannel: SendChannel<Int>
    ) : FlowFromActorMessage<Int>


    private fun CoroutineScope.launchWorker(
        channel: ReceiveChannel<Message>
    ) = launch {
        for (msg in channel) {
            handleFlowFromActorMessage(msg) { c ->
                c.send(1)
                c.send(2)
            }
        }
    }

    @Test
    fun `should return flow from actor`() {
        runBlocking {
            withTimeout(1000) {
                val channel = Channel<Message>()
                val worker = launchWorker(channel)
                val result = flowFromActor<Message, Int>(channel) { c, d -> Message(c, d) }.toList()
                assert(result == listOf(1, 2))
                worker.cancel()
            }
        }
    }

    data class MyMessage(
        override val cancelChannel: ReceiveChannel<Unit>,
        override val dataChannel: SendChannel<Int>
    ) : FlowFromActorMessage<Int>


    // TODO rewrite as an actual test
    @Test
    fun `otherTest`() {
        runBlocking {
            val ch = Channel<FlowFromActorMessage<Int>>()
            supervisorScope {
                val worker = launch {
                    try {
                        while (true) {
                            for (msg in ch) {
                                log("before handle")
                                handleFlowFromActorMessage(msg) { res ->
                                    log("dispatching 1")
                                    res.send(1)
                                    log("dispatched 1")
                                    delay(2000)
                                    error("poupa")
                                }
                                log("after handle")
                            }
                        }
                    } catch (e: Exception) {
                        log("worker exception $e")
                        throw e
                    }
                }

//        val first = launch {
//            flowFromActor<MyMessage, Int>(ch) { c, d -> MyMessage(c, d) }.collect { log(it); error("poupa") }
//        }
//
//        launch { delay(1000); first.cancel() }
                delay(200)
                val second = launch {
                    try {
                        flowFromActor<MyMessage, Int>(ch) { c, d ->
                            MyMessage(c, d)
                        }.collect {
                            log(it)
                            log("before delay in second collect")
                            delay(5000)
                            log("after delay in second collect")
                        }
                    } catch (e: Exception) {
                        log("collect exception $e")
                    }
                }

                delay(1000)
                worker.cancel()
                worker.join()
                second.join()
            }
        }
    }
}
