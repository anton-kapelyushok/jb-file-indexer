package home.pathfinder.indexing

import kotlinx.coroutines.flow.Flow

typealias Term = String
typealias DocumentName = String

interface Index<TermData : Any> {
    suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<TermData>>)
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
