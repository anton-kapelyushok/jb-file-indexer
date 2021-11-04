package home.pathfinder.indexing

import home.pathfinder.indexing.RootWatcherEvent.RootWatcherLifeCycleEvent

internal sealed interface RootWatcherState {

    class UnexpectedWatcherStopException : RuntimeException()
    class RootDeletedException : RuntimeException()

    data class Initializing(override val cancel: () -> Unit) : RootWatcherState, Cancelable {
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

        override fun asStatus() = RootWatcherStateInfo(RootWatcherStateInfo.Status.Initializing)

        override fun onTerminate(): RootWatcherState {
            cancel()
            return Canceling
        }

        override fun toString(): String {
            return "Initializing"
        }
    }

    data class Running(override val cancel: () -> Unit) : RootWatcherState, Cancelable {
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

        override fun asStatus() = RootWatcherStateInfo(RootWatcherStateInfo.Status.Running)

        override fun onTerminate(): RootWatcherState {
            cancel()
            return Canceling
        }

        override fun toString(): String {
            return "Running"
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

        override fun toString(): String {
            return "Failing, error: $error"
        }

        override fun asStatus() = RootWatcherStateInfo(RootWatcherStateInfo.Status.Failing, error)
    }

    data class Failed(val error: Throwable) : RootWatcherState {
        override val terminating: Boolean = false
        override val inConsistentState = true

        override fun onInterestCeased(): RootWatcherState? {
            return null
        }

        override fun toString(): String {
            return "Failed, error: $error"
        }

        override fun asStatus() = RootWatcherStateInfo(RootWatcherStateInfo.Status.Failed, error)
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

        override fun toString(): String {
            return "Canceling"
        }

        override fun asStatus() = RootWatcherStateInfo(RootWatcherStateInfo.Status.Cancelling)
    }

    interface Cancelable {
        val cancel: () -> Unit
    }

    val terminating: Boolean
    val inConsistentState: Boolean

    fun onTerminate(): RootWatcherState = this
    fun onWatcherEvent(event: RootWatcherLifeCycleEvent): RootWatcherState? = this
    fun onInterestCeased(): RootWatcherState? = this
    fun asStatus(): RootWatcherStateInfo
}

data class RootWatcherStateInfo(val status: Status, val exception: Throwable? = null) {
    enum class Status { Initializing, Running, Failing, Failed, Cancelling }
}
