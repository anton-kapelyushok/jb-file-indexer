package home.pathfinder.indexing

import home.pathfinder.indexing.RootWatcherEvent.RootWatcherLifeCycleEvent
import kotlinx.coroutines.CompletableDeferred

internal sealed interface RootWatcherState {

    class UnexpectedWatcherStopException : RuntimeException()
    class RootDeletedException : RuntimeException()

    data class Initializing(override val cancel: CompletableDeferred<Unit>) : RootWatcherState, Cancelable {
        override val terminating: Boolean = false
        override val inConsistentState: Boolean = false

        override fun onWatcherEvent(event: RootWatcherLifeCycleEvent): RootWatcherState {
            return when (event) {
                is RootWatcherEvent.Failed -> Failing(event.exception)
                is RootWatcherEvent.Initialized -> Running(cancel)
                is RootWatcherEvent.Overflown -> onTerminate()
                is RootWatcherEvent.StoppedWatching -> Failing(UnexpectedWatcherStopException())
                else -> this
            }
        }

        override fun onTerminate(): RootWatcherState {
            cancel.complete(Unit)
            return Canceling
        }
    }

    data class Running(override val cancel: CompletableDeferred<Unit>) : RootWatcherState, Cancelable {
        override val terminating: Boolean = false
        override val inConsistentState = true

        override fun onWatcherEvent(event: RootWatcherLifeCycleEvent): RootWatcherState {
            return when (event) {
                is RootWatcherEvent.Failed -> Failing(event.exception)
                is RootWatcherEvent.RootDeleted -> Failing(RootDeletedException())
                is RootWatcherEvent.Overflown -> onTerminate()
                is RootWatcherEvent.StoppedWatching -> Failing(UnexpectedWatcherStopException())
                else -> this
            }
        }

        override fun onTerminate(): RootWatcherState {
            cancel.complete(Unit)
            return Canceling
        }
    }

    data class Failing(val error: Throwable) : RootWatcherState {
        private var rootRemoveRequested = false

        override val terminating = true
        override val inConsistentState = false

        override fun onWatcherEvent(event: RootWatcherLifeCycleEvent): RootWatcherState? {
            return when (event) {
                is RootWatcherEvent.Stopped -> if (rootRemoveRequested) null else Failed(error)
                else -> this
            }
        }

        override fun onInterestCeased(): RootWatcherState {
            rootRemoveRequested = true
            return this
        }
    }

    data class Failed(val error: Throwable) : RootWatcherState {
        override val terminating: Boolean = false
        override val inConsistentState = true

        override fun onInterestCeased(): RootWatcherState? {
            return null
        }
    }

    object Canceling : RootWatcherState {
        override val terminating: Boolean = true
        override val inConsistentState = false

        override fun onWatcherEvent(event: RootWatcherLifeCycleEvent): RootWatcherState? {
            return when (event) {
                is RootWatcherEvent.Stopped -> null
                else -> this
            }
        }
    }

    interface Cancelable {
        val cancel: CompletableDeferred<Unit>
    }

    val terminating: Boolean
    val inConsistentState: Boolean

    fun onTerminate(): RootWatcherState = this
    fun onWatcherEvent(event: RootWatcherLifeCycleEvent): RootWatcherState? = this
    fun onInterestCeased(): RootWatcherState? = this
}
