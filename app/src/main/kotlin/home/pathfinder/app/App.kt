package home.pathfinder.app

import home.pathfinder.indexing.fileIndexer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@OptIn(ExperimentalTime::class)
fun main() {
    runBlocking {

//        val fileIndexer = fileIndexer { path ->
//            flow { emit(Posting(path.split("/").last(), 0)) }
//        }

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
                var inputRequested = false
                suspend fun readLine(): String {
                    if (!inputRequested) {
                        requestInput.send(Unit)
                    }
                    inputRequested = true
                    return consoleInput.receive().also { inputRequested = false }
                }

                while (true) {
                    println("Enter command (watch/find/status/exit)")

                    val input = readLine()
                    println()

                    val parts = input.split(" ").map { it.trim() }

                    when (val cmd = parts[0]) {

                        "watch" -> try {
                            fileIndexer.updateContentRoots(parts.subList(1, parts.size).toSet())
                        } catch (e: Throwable) {
                            println("error: $e")
                        }

                        "find" -> {
                            println("searching, press any key to cancel")
                            val searchJob = launch {
                                val (result, duration) = measureTimedValue {
                                    if (parts.size != 2) return@measureTimedValue emptyList()
                                    fileIndexer.searchExact(parts[1]).toList()
                                }
                                if (result.isEmpty()) println("Nothing found")
                                else {
                                    result.forEach { (documentName, term, lineNumber) ->
                                        println("$term: $documentName:$lineNumber")
                                    }
                                    println()
                                    println("Total ${result.size} entries")
                                }
                                println("Found in ${duration.inWholeMilliseconds}ms")
                                println()
                            }

                            val readlineJob = launch { readLine() }
                            select<Unit> {
                                readlineJob.onJoin {
                                    searchJob.cancel()
                                }
                                searchJob.onJoin {
                                    readlineJob.cancel()
                                }
                            }

                            searchJob.join()
                        }

                        "status" -> {
                            println(fileIndexer.state.value)
                            println()
                        }
                        "exit" -> {
                            break
                        }
                        else -> println("unknown cmd $cmd\n")
                    }
                }
            }
        } finally {
            requestInput.cancel()
            indexerJob.cancel()
            consoleReaderJob.cancel()
        }
    }
}
