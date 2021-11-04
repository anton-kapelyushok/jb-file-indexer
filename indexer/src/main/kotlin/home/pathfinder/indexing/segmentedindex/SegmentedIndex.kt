package home.pathfinder.indexing.segmentedindex

import home.pathfinder.indexing.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch

internal class SegmentedIndex(
    createSegmentFromFileConcurrency: Int,
    mergeSegmentsConcurrency: Int,
    targetSegmentsCount: Int,
) : Index<Int> {
    private val searchLockInput = Channel<Boolean>()
    private val documentUpdateInput = Channel<DocumentMessage>()
    private val searchInput = Channel<SearchExactMessage<Int>>()

    override suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<Int>>) {
        documentUpdateInput.send(DocumentMessage.Update(name, terms))
    }

    override suspend fun removeDocument(name: DocumentName) {
        documentUpdateInput.send(DocumentMessage.Remove(name))
    }

    override suspend fun setSearchLockStatus(status: Boolean) {
        searchLockInput.send(status)
    }

    override suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<Int>> =
        flowFromActor(searchInput) { cancel, data -> SearchExactMessage(term, data, cancel) }

    override suspend fun go(scope: CoroutineScope) =
        scope.launch { segmentedIndexCoordinator(_state, searchLockInput, documentUpdateInput, searchInput) }

    private val _state = MutableStateFlow(IndexStatusInfo.empty())
    override val state: StateFlow<IndexStatusInfo> get() = _state
}
