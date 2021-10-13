package home.pathfinder.app

import home.pathfinder.indexing.HashMapIndex
import home.pathfinder.indexing.Posting
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun main() {
    // TODO: convert to test
    runBlocking {
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
