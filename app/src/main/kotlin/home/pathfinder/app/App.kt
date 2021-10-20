package home.pathfinder.app

import home.pathfinder.indexing.fileIndexer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.system.measureTimeMillis
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@OptIn(ExperimentalTime::class)
fun main() {
    runBlocking {

//        val fileIndexer = fileIndexer { path ->
//            flow { emit(Posting(path.split("/").last(), 0)) }
//        }
        val fileIndexer = fileIndexer()

        launch(Dispatchers.Default) { fileIndexer.go(this) }

        fileIndexer.updateContentRoots(setOf("src/indexer"))
        val millis = measureTimeMillis { println(fileIndexer.searchExact("override").toList()) }
        println("Started in $millis")


        launch(Dispatchers.IO) {
            while (true) {
                println("enter command")
                val term = readLine()!!

                val parts = term.split(" ").map { it.trim() }

                when (val cmd = parts[0]) {
                    "watch" -> fileIndexer.updateContentRoots(parts.subList(1, parts.size).toSet())
                    "find" -> {
                        launch {
                            val (value, duration) = measureTimedValue {
                                if (parts.size != 2) return@measureTimedValue emptyList<Any>()
                                try {
                                    fileIndexer.searchExact(parts[1]).toList()
                                } catch (e: Throwable) {
                                    println("launch $e")
                                }
                            }
                            println(value)
                            println("Found in ${duration.inWholeMilliseconds}ms")
                        }
                    }
                    "status" -> {
                        println(fileIndexer.state.value)
                    }
                    else -> println("unknown cmd $cmd")
                }
            }
        }
    }
}
