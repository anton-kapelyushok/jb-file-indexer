package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.*
import home.pathfinder.indexing.RootWatcherEvent.*
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.createDirectory
import kotlin.io.path.createFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.writeLines

@Suppress("BlockingMethodInNonBlockingContext")
class RootWatcherTest {
    @Test
    fun `should watch file changes`() {
        fileSystemTest { workingDirectory ->
            val file = Paths.get(workingDirectory.toString(), "poupa.txt")
            file.createFile()
            delay(200) // let it flush

            runRootWatcher(file.toString()) { result, cancel ->
                val startedEvents = result.getEvents(2)
                assertThat(startedEvents.size).isEqualTo(2)
                assertThat(startedEvents[0]).isInstanceOf(FileUpdated::class)
                assertThat((startedEvents[0] as FileUpdated).path).endsWith("poupa.txt")
                assertThat(startedEvents[1]).isInstanceOf(Initialized::class)

                file.writeLines(sequence { yield("1") })
                val updatedEvents1 = result.getEvents(1)
                assertThat(updatedEvents1[0]).isInstanceOf(FileUpdated::class)
                assertThat((updatedEvents1[0] as FileUpdated).path).endsWith("poupa.txt")

                file.writeLines(sequence { yield("2") })
                val updatedEvents2 = result.getEvents(1)
                assertThat(updatedEvents2[0]).isInstanceOf(FileUpdated::class)
                assertThat((updatedEvents2[0] as FileUpdated).path).endsWith("poupa.txt")
                cancel.complete(Unit)

                val teardownEvents = result.getEvents(Int.MAX_VALUE)

                assertThat(teardownEvents.map { it.javaClass }).hasSize(3)

                assertThat(teardownEvents[0]).isInstanceOf(StoppedWatching::class)
                assertThat(teardownEvents[1]).isInstanceOf(FileDeleted::class)
                assertThat(teardownEvents[2]).isInstanceOf(Stopped::class)
            }
        }
    }

    @Test
    fun `should handle rootRemoved on file`() {
        fileSystemTest { workingDirectory ->
            val file = Paths.get(workingDirectory.toString(), "poupa.txt")
            file.createFile()
            delay(200) // let it flush

            runRootWatcher(file.toString()) { result, cancel ->
                result.getEvents(2)
                file.deleteExisting()

                val teardownEvents = result.getEvents(Int.MAX_VALUE)

                assertThat(teardownEvents.map { it.javaClass }).hasSize(4)

                assertThat(teardownEvents[0]).isInstanceOf(FileDeleted::class)
                assertThat(teardownEvents[1]).isInstanceOf(RootDeleted::class)
                assertThat(teardownEvents[2]).isInstanceOf(StoppedWatching::class)
                assertThat(teardownEvents[3]).isInstanceOf(Stopped::class)
            }
        }
    }

    @Test
    fun `should handle directory changes`() {
        fileSystemTest { workingDirectory ->
            val rootDir = Paths.get(workingDirectory.toString(), "poupa")
            rootDir.createDirectory()

            val poupaFile = Paths.get(rootDir.toString(), "poupa.txt").also { it.createFile() }
            val loupaFile = Paths.get(rootDir.toString(), "loupa.txt").also { it.createFile() }
            delay(200) // let it flush

            runRootWatcher(rootDir.toString()) { result, cancel ->
                val startedEvents = result.getEvents(3)

                assertThat(startedEvents.size).isEqualTo(3)

                val fileUpdateEvents = startedEvents.subList(0, 2)
                assertThat(fileUpdateEvents).each { it.isInstanceOf(FileUpdated::class) }
                assertThat(fileUpdateEvents.map { it as FileUpdated }.map { it.path.split(File.separator).last() })
                    .containsAll("poupa.txt", "loupa.txt")

                assertThat(startedEvents[2]).isInstanceOf(Initialized::class)

                poupaFile.writeLines(sequence { yield("1") })
                val poupaUpdateEvents = result.getEvents(1)

                assertThat(poupaUpdateEvents[0]).isInstanceOf(FileUpdated::class)
                assertThat((poupaUpdateEvents[0] as FileUpdated).path).endsWith("poupa.txt")

                loupaFile.deleteExisting()
                val loupaFileUpdates = result.getEvents(1)
                assertThat(loupaFileUpdates[0]).isInstanceOf(FileDeleted::class)
                assertThat((loupaFileUpdates[0] as FileDeleted).path).endsWith("loupa.txt")

                cancel.complete(Unit)

                val teardownEvents = result.getEvents(Int.MAX_VALUE)

                assertThat(teardownEvents.map { it.javaClass }).hasSize(3)

                assertThat(teardownEvents[0]).isInstanceOf(StoppedWatching::class)
                assertThat(teardownEvents[1]).isInstanceOf(FileDeleted::class)
                assertThat(teardownEvents[2]).isInstanceOf(Stopped::class)
            }
        }
    }

    @Test
    fun `should handle rootRemoved on directory`() {
        fileSystemTest { workingDirectory ->
            val rootDir = Paths.get(workingDirectory.toString(), "poupa")
            rootDir.createDirectory()

            runRootWatcher(rootDir.toString()) { result, cancel ->

                result.getEvents(1)

                rootDir.deleteExisting()

                val teardownEvents = result.getEvents(Int.MAX_VALUE)

                assertThat(teardownEvents.map { it.javaClass }).hasSize(3)

                assertThat(teardownEvents[0]).isInstanceOf(RootDeleted::class)
                assertThat(teardownEvents[1]).isInstanceOf(StoppedWatching::class)
                assertThat(teardownEvents[2]).isInstanceOf(Stopped::class)
            }
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

        suspend fun getEvents(numberOfEvents: Int): List<RootWatcherEvent> {
            val events = mutableListOf<RootWatcherEvent>()

            fun add(event: RootWatcherEvent) {
                println("$event")
                events.add(event)
            }

            while (running && events.size < numberOfEvents) {
                select<Unit> {
                    watcher.events.onReceive { event ->
                        when (event) {
                            is Stopped -> {
                                running = false
                            }
                            else -> Unit
                        }

                        add(event)
                    }

                    job.onJoin {
                        running = false
                    }
                }
            }

            return events
        }
    }
}
