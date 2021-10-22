package home.pathfinder.indexing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.nio.file.Files
import java.nio.file.Path

fun <T> fileSystemTest(timeout: Int = 10000, fn: suspend CoroutineScope.(dir: Path) -> T): T {
    val dir = Files.createTempDirectory("poupa")
    return try {
        runBlocking(Dispatchers.IO) {
            withTimeout(10000) {
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
