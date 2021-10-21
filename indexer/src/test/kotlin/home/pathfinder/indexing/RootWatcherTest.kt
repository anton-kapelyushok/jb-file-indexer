package home.pathfinder.indexing

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createDirectory
import kotlin.io.path.createFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.writeLines

class RootWatcherTest {
    @Test
    fun `should watch file changes`() {
        rootWatcherTest { workingDirectory ->
            val file = Paths.get(workingDirectory.toString(), "poupa.txt")
            file.createFile()
            delay(200) // let it flush

            runRootWatcher(file.toString()) { result, cancel ->

                val startedEvents = result.getEvents(2)
                assertThat(startedEvents.size).isEqualTo(2)
                assertThat(startedEvents[0].type).isEqualTo("fileUpdated")
                assertThat(startedEvents[0].payload as String).endsWith("poupa.txt")
                assertThat(startedEvents[1].type).isEqualTo("started")

                file.writeLines(sequence { yield("1") })
                val updatedEvents1 = result.getEvents(1)
                assertThat(updatedEvents1[0].type).isEqualTo("fileUpdated")
                assertThat(updatedEvents1[0].payload as String).endsWith("poupa.txt")

                file.writeLines(sequence { yield("2") })
                val updatedEvents2 = result.getEvents(1)
                assertThat(updatedEvents2[0].type).isEqualTo("fileUpdated")
                assertThat(updatedEvents2[0].payload as String).endsWith("poupa.txt")
                cancel.complete(Unit)

                val teardownEvents = result.getEvents(Int.MAX_VALUE)

                assertThat(teardownEvents.map { it.type }).all {
                    hasSize(3)
                    containsExactly("stoppedWatching", "fileRemoved", "jobFinished")
                }
            }
        }
    }

    @Test
    fun `should handle rootRemoved on file`() {
        rootWatcherTest { workingDirectory ->
            val file = Paths.get(workingDirectory.toString(), "poupa.txt")
            file.createFile()
            delay(200) // let it flush

            runRootWatcher(file.toString()) { result, cancel ->
                result.getEvents(2)
                file.deleteExisting()

                val teardownEvents = result.getEvents(Int.MAX_VALUE)

                assertThat(teardownEvents.map { it.type }).all {
                    hasSize(4)
                    containsExactly("fileRemoved", "rootRemoved", "stoppedWatching", "jobFinished")
                }
            }
        }
    }

    @Test
    fun `should handle directory changes`() {
        rootWatcherTest { workingDirectory ->
            val rootDir = Paths.get(workingDirectory.toString(), "poupa")
            rootDir.createDirectory()

            val poupaFile = Paths.get(rootDir.toString(), "poupa.txt").also { it.createFile() }
            val loupaFile = Paths.get(rootDir.toString(), "loupa.txt").also { it.createFile() }
            delay(200) // let it flush

            runRootWatcher(rootDir.toString()) { result, cancel ->
                val startedEvents = result.getEvents(3)

                assertThat(startedEvents.size).isEqualTo(3)

                val fileUpdateEvents = startedEvents.subList(0, 2)
                assertThat(fileUpdateEvents.map { it.type }).each { it.endsWith("fileUpdated") }
                assertThat(fileUpdateEvents.map { (it.payload as String).split("/").last() })
                    .containsAll(
                        "poupa.txt",
                        "loupa.txt"
                    )

                assertThat(startedEvents[2].type).isEqualTo("started")

                poupaFile.writeLines(sequence { yield("1") })
                val poupaUpdateEvents = result.getEvents(1)
                assertThat(poupaUpdateEvents[0].type).isEqualTo("fileUpdated")
                assertThat(poupaUpdateEvents[0].payload as String).endsWith("poupa.txt")

                loupaFile.deleteExisting()
                val loupaFileUpdates = result.getEvents(1)
                assertThat(loupaFileUpdates[0].type).isEqualTo("fileRemoved")
                assertThat(loupaFileUpdates[0].payload as String).endsWith("loupa.txt")

                cancel.complete(Unit)

                assertThat(result.getEvents(Int.MAX_VALUE).map { it.type }).all {
                    containsAll("stoppedWatching", "fileRemoved", "jobFinished")
                    hasSize(3)
                }

            }
        }
    }

    @Test
    fun `should handle rootRemoved on directory`() {
        rootWatcherTest { workingDirectory ->
            val rootDir = Paths.get(workingDirectory.toString(), "poupa")
            rootDir.createDirectory()

            runRootWatcher(rootDir.toString()) { result, cancel ->

                result.getEvents(1)

                rootDir.deleteExisting()

                val teardownEvents = result.getEvents(Int.MAX_VALUE)

                assertThat(teardownEvents.map { it.type }).all {
                    hasSize(3)
                    containsExactly("rootRemoved", "stoppedWatching", "jobFinished")
                }
            }
        }
    }

    private fun <T> rootWatcherTest(fn: suspend CoroutineScope.(dir: Path) -> T): T {
        val dir = Files.createTempDirectory("poupa")
        return try {
            runBlocking(Dispatchers.IO) {
                withTimeout(5_000) {
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

    private suspend fun <T> CoroutineScope.runRootWatcher(
        path: String,
        fn: suspend (result: RootWatcherResult, cancel: CompletableDeferred<Unit>) -> T
    ): T {
        val cancel = CompletableDeferred<Unit>()
        val rw = RootWatcher(path, cancel)
        val job = launch { rw.go(this) }

        return try {
            fn(RootWatcherResult(rw, job), cancel)
        } finally {
            cancel.complete(Unit)
            job.cancel()
        }
    }

    internal class RootWatcherResult(
        private val watcher: RootWatcher,
        private val job: Job,
    ) {

        var running = true

        suspend fun getEvents(numberOfEvents: Int): List<RootWatcherTestEvent> {
            val events = mutableListOf<RootWatcherTestEvent>()

            fun add(type: String, payload: Any) {
                println("$type $payload")
                events.add(RootWatcherTestEvent(type, payload))
            }

            while (running && events.size < numberOfEvents) {
                select<Unit> {
                    watcher.error.onReceive {
                        add("error", it)
                    }
                    watcher.started.onReceive {
                        add("started", it)
                    }
                    watcher.overflow.onReceive {
                        add("overflow", it)
                    }
                    watcher.rootRemoved.onReceive {
                        add("rootRemoved", it)
                    }
                    watcher.stoppedWatching.onReceive {
                        add("stoppedWatching", it)
                    }
                    job.onJoin {
                        add("jobFinished", Unit)
                        running = false
                    }
                    watcher.fileUpdated.onReceive {
                        add("fileUpdated", it)
                    }
                    watcher.fileRemoved.onReceive {
                        add("fileRemoved", it)
                    }
                }
            }

            return events
        }
    }

    data class RootWatcherTestEvent(val type: String, val payload: Any)
}
