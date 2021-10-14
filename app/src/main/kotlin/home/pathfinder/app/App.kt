package home.pathfinder.app

import home.pathfinder.indexing.FileIndexer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.system.measureTimeMillis

fun main() {
    // TODO: convert to test
    runBlocking {

        val fileIndexer = FileIndexer(listOf("."), listOf(""".*/\.git/.*""", """.*/build/.*""", """.*/gradle/.*"""))

        launch { fileIndexer.go(this) }

        val startTime = measureTimeMillis {
            delay(100)
            fileIndexer.searchExact("override").collect {  }
        }

        println("started in $startTime")

        launch(Dispatchers.IO) {
            while (true) {
                println("enter term")
                val term = readLine()!!
                fileIndexer.searchExact(term).onEach { println(it) }.collect()
            }
        }
    }
}
