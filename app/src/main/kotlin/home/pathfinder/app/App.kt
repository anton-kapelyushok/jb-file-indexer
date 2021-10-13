package home.pathfinder.app

import home.pathfinder.indexing.HashMapIndex
import home.pathfinder.indexing.Posting
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

fun main() {
    // TODO: convert to test
    runBlocking {
        val channel = Channel<String>()
        delay(200)
        val job1 = launch {
            try {
                println("job 1 launch")
                channel.send("1")
            } catch (e: Exception) {
                println("job 1 $e isActive = $isActive")
                delay(10000)
            }
        }

        val job2 = launch {
            try {
                println("job 2 launch")
                channel.send("2")
            } catch (e: Exception) {
                println("job 2 $e isActive = $isActive")
                delay(10000)
            }
        }


        delay(200L)

        job2.cancel()
        channel.cancel()

        job1.join()
        job2.join()


        delay(2000)
        error("return")


        val index = HashMapIndex<String>()

        val indexWorker = launch { index.go(this) }

        val doc = { docId: Int ->
            flow {
                try {
                    for (i in 1..5) {
                        println("doc$docId doc emit $i")
                        emit(Posting("term$i", "doc$docId $i"))
                        println("doc$docId start delay")
                        delay(i * 400L)
                    }
                    println("doc$docId flow finish")
                } finally {
                    println("doc$docId finally")
                }
            }
        }

        index.updateDocument("doc1", doc(1))
        index.updateDocument("doc2", doc(2))
        index.updateDocument("doc3", doc(3))
        index.updateDocument("doc1", doc(4))
        index.updateDocument("doc1", doc(5))

        index.searchExact("term2").collect { println(it) }

        indexWorker.cancel()
    }
}
