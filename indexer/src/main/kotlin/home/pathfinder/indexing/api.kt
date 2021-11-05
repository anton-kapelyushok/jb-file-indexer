package home.pathfinder.indexing

import home.pathfinder.indexing.segmentedindex.SegmentedIndex
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

typealias Term = String
typealias DocumentName = String

const val rwInitialEmitFromFileHasherHackDisabled = "rwInitialEmitFromFileHasherHackDisabled"

fun fileIndexer(
    index: Index<Int> = segmentedIndex(),
    tokenize: (String) -> Flow<Posting<Int>> = ::splitBySpace,
    additionalProperties: Map<String, Any> = mapOf(),
): FileIndexer = FileIndexerImpl(index, tokenize, additionalProperties)

fun hashMapIndex(): Index<Int> = HashMapIndex()
fun segmentedIndex(
    createSegmentFromFileConcurrency: Int = 16,
    mergeSegmentsConcurrency: Int = 4,
    targetSegmentsCount: Int = 32,
): Index<Int> = SegmentedIndex(
    createSegmentFromFileConcurrency,
    mergeSegmentsConcurrency,
    targetSegmentsCount,
)

interface FileIndexer : Actor {
    override suspend fun go(scope: CoroutineScope): Job

    /**
     * Updates current roots.
     * Function call returns (almost) immediately, update is scheduled.
     * Search is blocked until update comes through.
     */
    suspend fun updateContentRoots(newRoots: Set<String>, newIgnoredRoots: Set<String>)

    suspend fun searchExact(term: String): Flow<SearchResultEntry<Int>>

    /**
     * Contains information about index state and errors
     */
    val state: StateFlow<FileIndexerStatusInfo>
}


interface Index<TermData : Any> : Actor {
    suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<TermData>>)
    suspend fun removeDocument(name: DocumentName)
    suspend fun setSearchLockStatus(status: Boolean)
    suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<TermData>>

    val state: StateFlow<IndexStatusInfo>
}

interface Actor {
    suspend fun go(scope: CoroutineScope): Job
}

data class IndexStatusInfo(
    val ackedUpdates: Long,
    val searchLocked: Boolean,
    val runningUpdates: Int,
    val pendingUpdates: Int,
    val indexedDocuments: Int,
    val errors: Map<DocumentName, Throwable>,
    val segments: Int,
    val segmentMergesInProgress: Int,
) {
    companion object {
        fun empty() = IndexStatusInfo(
            ackedUpdates = 0L,
            searchLocked = false,
            pendingUpdates = 0,
            runningUpdates = 0,
            indexedDocuments = 0,
            errors = emptyMap(),
            segments = 0,
            segmentMergesInProgress = 0,
        )
    }
}

data class SearchResultEntry<TermData : Any>(
    val documentName: DocumentName,
    val term: Term,
    val termData: TermData,
)

data class Posting<TermData : Any>(
    val term: Term,
    val termData: TermData,
)

data class FileIndexerStatusInfo(
    val indexInfo: IndexStatusInfo,
    val watcherStates: Map<WatchedRoot, RootWatcherStateInfo>
) {
    companion object {
        fun empty() = FileIndexerStatusInfo(IndexStatusInfo.empty(), emptyMap())
    }

    override fun toString(): String {
        return """indexInfo: 
$indexInfo

rootStates:
${watcherStates.entries.joinToString("\n")}"""
    }
}
