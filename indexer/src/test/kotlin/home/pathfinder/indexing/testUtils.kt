package home.pathfinder.indexing

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.toList
import java.io.File
import java.nio.file.Files
import java.nio.file.Path

fun <T> fileSystemTest(timeoutMillis: Long = 10_000, fn: suspend CoroutineScope.(dir: Path) -> T): T {
    val dir = Files.createTempDirectory("poupa")
    return try {
        runBlocking(Dispatchers.IO) {
            withTimeout(timeoutMillis) {
                fn(dir)
            }
        }
    } finally {
        Files.walk(dir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete)
    }
}

suspend fun Index<Int>.searchExactOrdered(term: Term): List<SearchResultEntry<Int>> {
    return this.searchExact(term).toList().sortedWith(compareBy({ it.documentName }, { it.term }, { it.termData }))
}

fun <T> runIndexTest(
    index: Index<Int>,
    timeoutMillis: Long = 1000L,
    fn: suspend CoroutineScope.(index: Index<Int>) -> T
): T {
    return runBlocking {
        withTimeout(timeoutMillis) {
            val indexJob = launch { index.go(this) }
            try {
                fn(index)
            } finally {
                indexJob.cancel()
            }
        }
    }
}
