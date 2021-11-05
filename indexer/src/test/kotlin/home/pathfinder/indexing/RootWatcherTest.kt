package home.pathfinder.indexing

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


            val otherFile = Paths.get(workingDirectory.toString(), "loupa.txt")
            otherFile.createFile()

            runRootWatcher(workingDirectory.toString(), file.toString()) { rwMatcher, cancelRootWatcher ->
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileUpdated && it.shortFileName() == "poupa.txt" }
                )
                rwMatcher.waitUntilEvent(
                    eventCondition { it is Initialized }
                )

                file.writeLines(sequence { yield("1") })
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileUpdated && it.shortFileName() == "poupa.txt" }
                )

                file.writeLines(sequence { yield("2") })
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileUpdated && it.shortFileName() == "poupa.txt" }
                )

                otherFile.writeLines(sequence { yield("3") })
                rwMatcher.verifyEventDoesNotHappenInPeriod(timeoutMillis = 500,
                    eventCondition { it is FileUpdated && it.shortFileName() == "loupa.txt" }
                )

                cancelRootWatcher()
                rwMatcher.waitUntilEvent(
                    eventCondition { it is StoppedWatching }
                )
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileDeleted && it.shortFileName() == "poupa.txt" }
                )
                rwMatcher.waitUntilEvent(
                    eventCondition { it is Stopped }
                )
            }
        }
    }

    @Test
    fun `should handle rootRemoved on file`() {
        fileSystemTest { workingDirectory ->
            val file = Paths.get(workingDirectory.toString(), "poupa.txt")
            file.createFile()

            runRootWatcher(workingDirectory.toString(), file.toString()) { rwMatcher, cancel ->
                rwMatcher.waitUntilEvent(
                    eventCondition { it is Initialized }
                )

                file.deleteExisting()

                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileDeleted && it.shortFileName() == "poupa.txt" }
                )

                rwMatcher.verifyEventDoesNotHappenInPeriod(timeoutMillis = 500,
                    eventCondition { it is StoppedWatching }
                )
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

            runRootWatcher(rootDir.toString()) { rwMatcher, cancelRootWatcher ->
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileUpdated && it.shortFileName() == "poupa.txt" },
                    eventCondition { it is FileUpdated && it.shortFileName() == "loupa.txt" },
                )

                rwMatcher.waitUntilEvent(
                    eventCondition { it is Initialized }
                )

                poupaFile.writeLines(sequence { yield("1") })
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileUpdated && it.shortFileName() == "poupa.txt" }
                )

                loupaFile.deleteExisting()
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileDeleted && it.shortFileName() == "loupa.txt" }
                )

                cancelRootWatcher()

                rwMatcher.waitUntilEvent(
                    eventCondition { it is StoppedWatching }
                )
                rwMatcher.waitUntilEvent(
                    eventCondition { it is FileDeleted && it.shortFileName() == "poupa.txt" }
                )
                rwMatcher.waitUntilEvent(
                    eventCondition { it is Stopped }
                )
            }
        }
    }

    @Test
    fun `should handle rootRemoved on directory`() {
        fileSystemTest { workingDirectory ->
            val rootDir = Paths.get(workingDirectory.toString(), "poupa")
            rootDir.createDirectory()

            runRootWatcher(rootDir.toString()) { rwMatcher, _ ->

                rwMatcher.waitUntilEvent(
                    eventCondition { it is Initialized }
                )

                rootDir.deleteExisting()
                rwMatcher.waitUntilEvent(
                    eventCondition { it is StoppedWatching }
                )
                rwMatcher.waitUntilEvent(
                    eventCondition { it is Stopped }
                )
            }
        }
    }

    private suspend fun <T> CoroutineScope.runRootWatcher(
        path: String, actualPath: String = path,
        fn: suspend (result: RootWatcherResult, cancel: () -> Unit) -> T
    ): T {
        val rw = RootWatcher(WatchedRoot(path, setOf(), setOf(actualPath)))
        val job = launch { rw.go(this) }

        return try {
            fn(RootWatcherResult(rw, job)) { rw.cancel() }
        } finally {
            rw.cancel()
            job.cancel()
        }
    }

    private fun RootWatcherFileEvent.shortFileName(): String = path.split(File.separator).last()

    private var conditionId = 0

    private data class EventCondition(
        val description: String,
        val predicate: (event: RootWatcherEvent) -> Boolean,
    ) {
        override fun toString(): String {
            return "Event condition: $description"
        }
    }

    private fun eventCondition(description: String? = null, predicate: (event: RootWatcherEvent) -> Boolean) =
        EventCondition(
            description = description ?: "${++conditionId}",
            predicate = predicate
        )

    private class RootWatcherResult(
        private val watcher: RootWatcher,
        private val job: Job,
    ) {

        private var running = true

        suspend fun verifyEventDoesNotHappenInPeriod(timeoutMillis: Long, condition: EventCondition) {
            if (!running) return
            coroutineScope {
                val delayJob = launch { delay(timeoutMillis) }
                val assertJob = async {
                    var matched = false
                    while (running && !matched) {
                        select<Unit> {
                            watcher.events.onReceive { event ->
                                println("$event")
                                if (condition.predicate(event)) matched = true
                                if (event is Stopped) {
                                    running = false
                                }
                            }
                            job.onJoin {
                                running = false
                            }
                        }
                    }
                    matched
                }

                select<Unit> {
                    delayJob.onJoin {
                        assertJob.cancel()
                        assertJob.join()
                    }
                    assertJob.onAwait { matched ->
                        delayJob.cancel()
                        delayJob.join()
                        if (matched) error("Event condition $condition matched in $timeoutMillis")
                    }
                }
            }
        }

        suspend fun waitUntilEvent(vararg conditions: EventCondition, timeoutMillis: Long = 2000L) {
            if (!running) error("Watcher stopped before conditions were satisfied")
            val remainingConditions = conditions.toMutableSet()
            try {
                withTimeout(timeoutMillis) {
                    while (running && remainingConditions.isNotEmpty()) {
                        select<Unit> {
                            watcher.events.onReceive { event ->
                                println("$event")
                                val matchedCondition = conditions.find { it.predicate(event) }
                                if (matchedCondition != null) remainingConditions -= matchedCondition
                                if (event is Stopped) {
                                    running = false
                                }
                            }
                            job.onJoin {
                                running = false
                            }
                        }
                    }

                    if (remainingConditions.isNotEmpty()) {
                        error("Watcher stopped before all conditions were satisfied, remaining conditions: $remainingConditions")
                    }
                }
            } catch (e: TimeoutCancellationException) {
                error("Conditions were not satisfied in $timeoutMillis milliseconds, remaining conditions: $remainingConditions")
            }
        }
    }
}
