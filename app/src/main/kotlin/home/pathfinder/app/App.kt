package home.pathfinder.app

import home.pathfinder.indexing.fileIndexer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.selects.select
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis

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

                        "watch" -> try {
                            val (ignoredRoots, roots) = parts.subList(1, parts.size).partition { it.startsWith("-") }

                            fileIndexer.updateContentRoots(roots.toSet(), ignoredRoots.map { it.substring(1) }.toSet())
                        } catch (e: Throwable) {
                            println("error: $e")
                        }

                        "find" -> {
                            println("searching, press any key to cancel")
                            val searchJob = launch {
                                var count = 0
                                val duration = measureTimeMillis {
                                    if (parts.size != 2) return@measureTimeMillis
                                    fileIndexer.searchExact(parts[1])
                                        .collect { (documentName, term, lineNumber) ->
                                            println("$term: $documentName:$lineNumber")
                                            count++
                                        }
                                }
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
        } finally {
            requestInput.cancel()
            indexerJob.cancel()
            consoleReaderJob.cancel()
        }
    }
}
