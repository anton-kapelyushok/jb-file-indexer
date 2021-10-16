package home.pathfinder.indexing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job

interface Actor {
    suspend fun go(scope: CoroutineScope): Job
}
