package home.pathfinder.indexing

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

typealias Term = String
typealias DocumentName = String

interface Index<TermData : Any> {
    suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<TermData>>)
    suspend fun removeDocument(name: DocumentName)
    suspend fun setSearchLockStatus(status: Boolean)

    val state: StateFlow<IndexStatusInfo>
}

data class IndexStatusInfo(
    val searchLocked: Boolean,
    val runningUpdates: Int,
    val pendingUpdates: Int,
    val indexedDocuments: Int,
    val errors: Map<DocumentName, Throwable>
) {
    companion object {
        fun empty() = IndexStatusInfo(
            searchLocked = false,
            pendingUpdates = 0,
            runningUpdates = 0,
            indexedDocuments = 0,
            errors = emptyMap()
        )
    }
}

interface SearchExact<TermData : Any> {
    suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<TermData>>
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
