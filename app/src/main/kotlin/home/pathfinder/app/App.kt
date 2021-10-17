package home.pathfinder.app

import home.pathfinder.indexing.FileIndexerImpl
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun main() {
    runBlocking {

        val fileIndexer = FileIndexerImpl()

        launch { fileIndexer.go(this) }

        launch {
            fileIndexer.updateContentRoots(setOf("indexer/src/main"))
            fileIndexer.searchExact("override").collect { println(it) }
        }

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
                    "search" -> fileIndexer.searchExact(parts[1]).collect { println(it) }
                    else -> println("unknown cmd $cmd")
                }
            }
        }
    }
}