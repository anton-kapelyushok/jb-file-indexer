package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import home.pathfinder.indexing.RootWatcherEvent.*
import home.pathfinder.indexing.RootWatcherEvent.Failed
import home.pathfinder.indexing.RootWatcherState.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class RootWatcherStateTest {
    @Test
    fun `happy path`() {
        runBlocking {
            var (state, cancel) = initialState()

            state = state!!.onWatcherEvent(Initialized)
            assertThat(state).isNotNull().isInstanceOf(Running::class)

            state = state!!.onTerminate()
            assertThat(state).isNotNull().isInstanceOf(Canceling::class)
            assertThat(cancel.isCompleted)

            state = state.onWatcherEvent(StoppedWatching)
            assertThat(state).isNotNull().isInstanceOf(Canceling::class)

            state = state!!.onWatcherEvent(Stopped)
            assertThat(state).isNull()
        }
    }

    @Test
    fun `failed on start`() {
        var (state, cancel) = initialState()

        state = state!!.onWatcherEvent(Initialized)
        assertThat(state).isNotNull().isInstanceOf(Running::class)

        state = state!!.onWatcherEvent(Failed(RuntimeException()))
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onWatcherEvent(StoppedWatching)
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onWatcherEvent(Stopped)
        assertThat(state).isNotNull().isInstanceOf(RootWatcherState.Failed::class)
    }

    @Test
    fun `failed in process`() {
        var (state, cancel) = initialState()

        state = state!!.onWatcherEvent(Failed(RuntimeException()))
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onWatcherEvent(StoppedWatching)
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onWatcherEvent(Stopped)
        assertThat(state).isNotNull().isInstanceOf(RootWatcherState.Failed::class)
    }

    @Test
    fun `root removed`() {
        var (state, cancel) = initialState()

        state = state!!.onWatcherEvent(Initialized)
        assertThat(state).isNotNull().isInstanceOf(Running::class)

        state = state!!.onWatcherEvent(RootDeleted)
        assertThat(state).isNotNull().isInstanceOf(Failing::class)
        assertThat((state!! as Failing).error).isInstanceOf(RootDeletedException::class)

        state = state!!.onWatcherEvent(StoppedWatching)
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onWatcherEvent(Stopped)
        assertThat(state).isNotNull().isInstanceOf(RootWatcherState.Failed::class)
    }

    @Test
    fun `overflown`() {
        var (state, cancel) = initialState()

        state = state!!.onWatcherEvent(Initialized)
        assertThat(state).isNotNull().isInstanceOf(Running::class)

        state = state!!.onWatcherEvent(Overflown)
        assertThat(state).isNotNull().isInstanceOf(Canceling::class)

        state = state!!.onWatcherEvent(Overflown)
        assertThat(state).isNotNull().isInstanceOf(Canceling::class)

        state = state!!.onWatcherEvent(StoppedWatching)
        assertThat(state).isNotNull().isInstanceOf(Canceling::class)

        state = state!!.onWatcherEvent(Stopped)
        assertThat(state).isNull()
    }

    @Test
    fun `unexpected watcher stop`() {
        var (state, cancel) = initialState()

        state = state!!.onWatcherEvent(Initialized)
        assertThat(state).isNotNull().isInstanceOf(Running::class)

        state = state!!.onWatcherEvent(StoppedWatching)
        assertThat(state).isNotNull().isInstanceOf(Failing::class)
        assertThat((state!! as Failing).error).isInstanceOf(UnexpectedWatcherStopException::class)


        state = state!!.onWatcherEvent(Stopped)
        assertThat(state).isNotNull().isInstanceOf(RootWatcherState.Failed::class)
    }

    @Test
    fun `failed but has ceased to interest while in failing state`() {
        var (state, cancel) = initialState()

        state = state!!.onWatcherEvent(Initialized)
        assertThat(state).isNotNull().isInstanceOf(Running::class)

        state = state!!.onWatcherEvent(Failed(RuntimeException()))
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onWatcherEvent(StoppedWatching)
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onInterestCeased()
        assertThat(state).isNotNull().isInstanceOf(Failing::class)

        state = state!!.onWatcherEvent(Stopped)
        assertThat(state).isNull()
    }

    private fun initialState(): Pair<RootWatcherState?, CompletableDeferred<Unit>> {
        val cancel = CompletableDeferred<Unit>()
        return Initializing(cancel) to cancel
    }
}
