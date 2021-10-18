package home.pathfinder.app

import home.pathfinder.indexing.FileIndexerImpl
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@OptIn(ExperimentalTime::class)
fun main() {
    runBlocking {

        val fileIndexer = FileIndexerImpl()

        launch { fileIndexer.go(this) }

        launch(Dispatchers.IO) {
            while (true) {
                println("enter command")
                val term = readLine()!!

                val parts = term.split(" ").map { it.trim() }
                if (parts.size < 2) {
                    println("Unknown command")
                    continue
                }

                when (val cmd = parts[0]) {
                    "roots" -> fileIndexer.updateContentRoots(parts.subList(1, parts.size).toSet())
                    "search" -> {
                        launch {
                            val (value, duration) = measureTimedValue {
                                fileIndexer.searchExact(parts[1]).toList()
                            }
                            println(value)
                            println("Found in ${duration.inWholeMilliseconds}ms")
                        }
                    }
                    else -> println("unknown cmd $cmd")
                }
            }
        }
    }
}
