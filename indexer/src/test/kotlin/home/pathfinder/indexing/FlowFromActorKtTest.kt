package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
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

    @Test
    fun `should return flow from actor`() {
        runBlocking {
            withTimeout(1000) {
                val channel = Channel<Message>()
                launch {
                    handleFlowFromActorMessage(channel.receive()) { c ->
                        c.send(1)
                        c.send(2)
                        c.send(3)
                    }
                }

                val result = flowFromActor<Message, Int>(channel) { c, d -> Message(c, d) }.toList()
                assertThat(result).isEqualTo(listOf(1, 2, 3))

                channel.close()
            }
        }
    }

    @Test
    fun `should cancel actor job on flow cancel`() {
        runBlocking {
            withTimeout(1000) {
                val channel = Channel<Message>()
                val started = Channel<Unit>(CONFLATED)
                launch {
                    handleFlowFromActorMessage(channel.receive()) { c ->
                        started.send(Unit)
                        delay(1_000_000)
                        c.send(1)
                    }
                }

                val readJob = launch { flowFromActor<Message, Int>(channel) { c, d -> Message(c, d) }.toList() }
                started.receive()
                readJob.cancel()
            }
        }
    }

    @Test
    fun `should cancel actor job on flow collect exception`() {
        class CollectException : RuntimeException()

        runBlocking {
            withTimeout(1000) {
                val channel = Channel<Message>()
                launch {
                    handleFlowFromActorMessage(channel.receive()) { c ->
                        c.send(1)
                        delay(1_000_000)
                    }

                }

                launch {
                    try {
                        flowFromActor<Message, Int>(channel) { c, d -> Message(c, d) }.collect {
                            throw CollectException()
                        }
                    } catch (e: Throwable) {
                        // ignore
                    }
                }
            }
        }
    }

    @Test
    fun `search should throw on actor cancel`() {
        runBlocking {
            withTimeout(1000) {
                val channel = Channel<Message>()
                val started = Channel<Unit>(CONFLATED)

                val workerJob = launch {
                    handleFlowFromActorMessage(channel.receive()) { c ->
                        started.send(Unit)
                        delay(1_000_000)
                        c.send(1)
                    }
                }

                var exception: Throwable? = null
                val readJob = launch {
                    try {
                        flowFromActor<Message, Int>(channel) { c, d -> Message(c, d) }.toList()
                    } catch (e: Throwable) {
                        assertThat(isActive).isEqualTo(true)
                        exception = e
                    }
                }
                started.receive()
                workerJob.cancel()
                readJob.join()

                assertThat(exception).isNotNull().isInstanceOf(CancellationException::class)
            }
        }
    }

    @Test
    fun `search should return exception from actor`() {
        class ActorException : RuntimeException()

        runBlocking {
            withTimeout(1000) {
                val channel = Channel<Message>()
                val started = Channel<Unit>(CONFLATED)

                launch {
                    try {
                        handleFlowFromActorMessage(channel.receive()) { c ->
                            started.send(Unit)
                            throw ActorException()
                        }
                    } catch (e: Throwable) {
                        // ignore
                    }
                }

                var exception: Throwable? = null
                val readJob = launch {
                    try {
                        val data = flowFromActor<Message, Int>(channel) { c, d -> Message(c, d) }.toList()
                        println(data)
                    } catch (e: Throwable) {
                        assertThat(isActive).isEqualTo(true)
                        exception = e
                    }
                }
                started.receive()
                readJob.join()

                assertThat(exception).isNotNull().isInstanceOf(ActorException::class)
            }
        }
    }
}
