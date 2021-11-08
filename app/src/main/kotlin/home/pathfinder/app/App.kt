package home.pathfinder.app

import home.pathfinder.indexing.FileIndexer
import home.pathfinder.indexing.fileIndexer
import home.pathfinder.indexing.rwInitialEmitFromFileHasherHackEnabled
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.selects.select
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis

fun main() {
    runBlocking {

//        val fileIndexer1 = fileIndexer(
//            index = home.pathfinder.indexing.segmentedIndex(
//                createSegmentFromFileConcurrency = 4,
//                mergeSegmentsConcurrency = 2,
//                targetSegmentsCount = 2
//            ),
//            tokenize = { path ->
//                kotlinx.coroutines.flow.flow { emit(home.pathfinder.indexing.Posting(path.split("/").last(), 0)) }
//            },
//            additionalProperties = mapOf(home.pathfinder.indexing.rwInitialEmitFromFileHasherHackEnabled to true)
//        )

        val fileIndexer = fileIndexer()

        val indexerJob = launch(Dispatchers.Default) { fileIndexer.go(this) }

        val requestInput = Channel<Unit>()
        val consoleInput = Channel<String>()
        val consoleReaderJob = launch(Dispatchers.IO) {
            for (req in requestInput) {
                val read = readLine()
                if (read == null) {
                    consoleInput.close()
                    break
                }
                consoleInput.send(read)
            }
            consoleInput.close()
        }

        try {
            withContext(Dispatchers.IO) {
                val inputRequested = AtomicBoolean(false)
                suspend fun readLine(): String {
                    if (!inputRequested.get()) {
                        requestInput.send(Unit)
                    }
                    inputRequested.set(true)
                    return consoleInput.receive().also { inputRequested.set(false) }
                }

                while (true) {
                    println("Enter command (watch/find/status/exit/pollstatus)")

                    val input = readLine()
                    println()

                    val parts = input.split(" ").map { it.trim() }.filter { it.isNotEmpty() }
                    if (parts.isEmpty()) continue

                    when (val cmd = parts[0]) {

                        "watch" -> watch(fileIndexer, parts.subList(1, parts.size))

                        "watch+find" -> {
                            watch(fileIndexer, parts.subList(1, parts.size))
                            find(fileIndexer, "poupa", ::readLine)
                        }

                        "find" -> {
                            find(fileIndexer, parts[1], ::readLine)
                        }

                        "status" -> {
                            println(fileIndexer.state.value)
                            println()
                        }

                        "pollstatus" -> {
                            val pollJob = launch {
                                while (true) {
                                    println("###############")
                                    println(fileIndexer.state.value)
                                    println()
                                    delay(1000)
                                }
                            }
                            readLine()
                            pollJob.cancel()
                            pollJob.join()
                        }
                        "exit" -> {
                            break
                        }
                        else -> println("unknown cmd $cmd\n")
                    }
                }
            }
        } catch (e: Throwable) {
            e.printStackTrace()
            throw e
        } finally {
            requestInput.cancel()
            indexerJob.cancel()
            consoleReaderJob.cancel()
        }
    }
}

private suspend fun CoroutineScope.find(
    fileIndexer: FileIndexer,
    term: String,
    readLine: suspend () -> String,
) {
    println("searching, press any key to cancel")
    val searchJob = launch {
        val statusJob = launch {
            var timeElapsed = 0
            while (true) {
                delay(1000)
                println("############### ${++timeElapsed}")
                println(fileIndexer.state.value)
                println()
            }
        }

        var count = 0
        val duration = measureTimeMillis {
            fileIndexer.searchExact(term)
                .collect { (documentName, term, lineNumber) ->
                    println("$term: $documentName:$lineNumber")
                    count++
                }
        }
        statusJob.cancel()
        if (count == 0) println("Nothing found")
        else {
            println()
            println("Total $count entries")
        }
        println("Found in ${duration}ms")
        println()
    }

    val readlineJob = launch { readLine() }
    select<Unit> {
        readlineJob.onJoin {
            searchJob.cancel()
            searchJob.join()
        }
        searchJob.onJoin {
            readlineJob.cancel()
            readlineJob.join()
        }
    }

    searchJob.join()
}

private suspend fun watch(
    fileIndexer: FileIndexer,
    input: List<String>
) {
    try {
        val (ignoredRoots, roots) = input.partition { it.startsWith("-") }

        fileIndexer.updateContentRoots(roots.toSet(), ignoredRoots.map { it.substring(1) }.toSet())
    } catch (e: Throwable) {
        println("error: $e")
    }
}
