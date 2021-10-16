package home.pathfinder.app

import home.pathfinder.indexing.FileIndexer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.system.measureTimeMillis

fun main() {
    // TODO: convert to test
    runBlocking {

        val fileIndexer = FileIndexer(listOf(""".*/\.git/.*""", """.*/build/.*""", """.*/gradle/.*"""))

        launch { fileIndexer.go(this) }

        fileIndexer.add("indexer/src")

        println("here")
        val startTime = measureTimeMillis {
            delay(100)
            println(fileIndexer.searchExact("override").toList().size)
        }

        println("started in $startTime")

        launch(Dispatchers.IO) {
            while (true) {
                println("enter term")
                val term = readLine()!!

                if (term == "remove") {
                    fileIndexer.remove("indexer/src")
                } else {
                    fileIndexer.searchExact(term).onEach { println(it) }.collect()
                }
            }
        }
    }
}
