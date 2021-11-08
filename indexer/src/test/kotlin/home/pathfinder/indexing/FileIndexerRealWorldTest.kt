package home.pathfinder.indexing

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.system.measureTimeMillis

private val workingDirRoot = "/Users/akapelyushok/Projects/intellij-community"

class FileIndexerRealWorldTest {
    @Test
    @Disabled
    fun `time until first search`() {
        runBlocking {
            val indexer = fileIndexer(
                segmentedIndex(),
                additionalProperties = mapOf(rwInitialEmitFromFileHasherHackEnabled to false)
            )
            val job = launch(Dispatchers.Default) { indexer.go(this) }

            try {
                indexer.updateContentRoots(setOf(), setOf())
                val millis = measureTimeMillis {
                    val statusJob = launch {
                        var seconds = 0
                        while (true) {
                            println("#### secondsElapsed: $seconds #####")
                            println(indexer.state.value)
                            println("#############################")
                            println()
                            delay(1000)
                            seconds++
                        }
                    }
                    indexer.updateContentRoots(setOf(workingDirRoot), setOf())
                    indexer.searchExact("poupa").collect()
                    statusJob.cancel()
                }
                println("Completed in $millis millis")

            } finally {
                job.cancel()
            }
        }
    }

}
