package home.pathfinder.indexing.segmentedindex

import home.pathfinder.indexing.Posting
import home.pathfinder.indexing.SearchResultEntry
import java.util.concurrent.atomic.AtomicLong

private val segmentIdSequence = AtomicLong(0L)

@Suppress("ArrayInDataClass")
internal data class SegmentState(
    val id: Long,

    val documents: Array<String>,
    val documentsState: BooleanArray,

    val termData: ByteArray,
    val termOffsets: IntArray,

    val dataTermIds: IntArray,
    val dataDocIds: IntArray,
    val dataTermData: IntArray,

    val postingsPerDocument: IntArray,
    val alivePostings: Int,
) {
    override fun hashCode(): Int {
        return super.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return this === other
    }
}

internal fun SegmentState.memoryConsumption(): Long {
    var result = 0L

    result += termData.size
    result += termOffsets.size
    result += dataTermIds.size * 4
    result += dataDocIds.size * 4
    result += dataTermData.size * 4

    return result
}

internal fun SegmentState.alivePostingsFraction(): Double {
    if (alivePostings == 0) return 0.0
    return alivePostings.toDouble() / dataTermIds.size
}

internal fun createSegment(document: String, documentData: List<Posting<Int>>): SegmentState {
//    return SegmentState(segmentIdSequence.incrementAndGet(), arrayOf(), booleanArrayOf(), byteArrayOf(), intArrayOf(), intArrayOf(), intArrayOf(), intArrayOf(), intArrayOf(), 0)

    val uniqueTerms = documentData.map { it.term }.toSet().sorted()

    val dataTermIds = IntArray(documentData.size)
    val dataDocIds = IntArray(documentData.size)
    val dataTermData = IntArray(documentData.size)

    documentData
        .sortedBy { it.term }
        .forEachIndexed { idx, (term, termData) ->
            val termId = uniqueTerms.binarySearch(term)
            dataTermIds[idx] = termId
            dataDocIds[idx] = 0
            dataTermData[idx] = termData
        }

    val (termData, termOffsets) = toStringData(uniqueTerms)

    return SegmentState(
        id = segmentIdSequence.incrementAndGet(),

        documents = arrayOf(document),
        documentsState = booleanArrayOf(true),

        termData = termData,
        termOffsets = termOffsets,

        dataTermIds = dataTermIds,
        dataDocIds = dataDocIds,
        dataTermData = dataTermData,

        postingsPerDocument = intArrayOf(documentData.size),
        alivePostings = documentData.size
    )
}

private fun toStringData(uniqueTerms: List<String>): Pair<ByteArray, IntArray> {
    val termsAsBytes = uniqueTerms.map { it.toByteArray() }
    val termData = ByteArray(termsAsBytes.sumOf { it.size })
    val termOffsets = IntArray(uniqueTerms.size)
    for (i in termsAsBytes.indices) {
        if (i > 0) termOffsets[i] = termOffsets[i - 1] + termsAsBytes[i - 1].size
        System.arraycopy(termsAsBytes[i], 0, termData, termOffsets[i], termsAsBytes[i].size)
    }
    return termData to termOffsets
}

internal fun SegmentState.deleteDocument(document: String): SegmentState {
    val docId = documents.binarySearch(document)
    return copy(
        documentsState = documentsState.copyOf().also { it[docId] = false },
        alivePostings = alivePostings - postingsPerDocument[docId]
    )
}

internal fun SegmentState.getAliveDocuments(): List<String> {
    return documents.indices.mapNotNull { if (documentsState[it]) documents[it] else null }
}

internal fun mergeSegments(segment1: SegmentState, segment2: SegmentState): SegmentState {
    val newDocuments = (segment1.getAliveDocuments() + segment2.getAliveDocuments()).toSortedSet().toTypedArray()

    val docIdLookupMap = newDocuments.indices.associateBy { newDocuments[it] }

    val newAlivePostings = segment1.alivePostings + segment2.alivePostings

    val newDataTermIds = IntArray(newAlivePostings)
    val newDataDocIds = IntArray(newAlivePostings)
    val newDataTermData = IntArray(newAlivePostings)

    val newPostingsPerDocument = IntArray(newDocuments.size)

    val newTerms = mutableListOf<String>()

    var inserted = 0

    class SegmentExt(val segment: SegmentState) {
        val docIdLookup = segment.documents.map { docIdLookupMap[it] ?: -1 }

        var i = 0

        var lastParsedTermId = -1
        var lastParsedTerm = ""

        fun isPostingDocumentAlive() = segment.documentsState[segment.dataDocIds[i]]

        fun reachedEnd() = i >= segment.dataTermIds.size

        fun moveToNextPosting() = i++

        fun addCurrentPosting() {
            val term = currentTerm()
            if (newTerms.isEmpty() || term != newTerms.last()) {
                newTerms.add(term)
            }
            val termId = newTerms.size - 1
            val docId = docIdLookup[segment.dataDocIds[i]]
            val termData = segment.dataTermData[i]

            newDataTermIds[inserted] = termId
            newDataDocIds[inserted] = docId
            newDataTermData[inserted] = termData

            newPostingsPerDocument[docId]++

            inserted++
        }

        fun currentTerm(): String {
            val termId = segment.dataTermIds[i]
            return if (termId == lastParsedTermId) lastParsedTerm
            else {
                segment.termAt(termId).also {
                    lastParsedTermId = termId
                    lastParsedTerm = it
                }
            }
        }
    }

    val seg1 = SegmentExt(segment1)
    val seg2 = SegmentExt(segment2)

    run {
        while (!seg1.reachedEnd() || !seg2.reachedEnd()) {
            when {
                !seg1.reachedEnd() && !seg1.isPostingDocumentAlive() -> seg1.moveToNextPosting()
                !seg2.reachedEnd() && !seg2.isPostingDocumentAlive() -> seg2.moveToNextPosting()
                seg1.reachedEnd() -> {
                    seg2.addCurrentPosting()
                    seg2.moveToNextPosting()
                }
                seg2.reachedEnd() -> {
                    seg1.addCurrentPosting()
                    seg1.moveToNextPosting()
                }
                else -> {
                    if (seg1.currentTerm() < seg2.currentTerm()) {
                        seg1.addCurrentPosting()
                        seg1.moveToNextPosting()
                    } else {
                        seg2.addCurrentPosting()
                        seg2.moveToNextPosting()
                    }
                }
            }
        }
    }

    val (termData, termOffsets) = toStringData(newTerms)

    return SegmentState(
        id = segmentIdSequence.incrementAndGet(),

        documents = newDocuments,
        documentsState = BooleanArray(newDocuments.size) { true },
        termData = termData,
        termOffsets = termOffsets,

        dataTermIds = newDataTermIds,
        dataDocIds = newDataDocIds,
        dataTermData = newDataTermData,

        postingsPerDocument = newPostingsPerDocument,
        alivePostings = newAlivePostings,
    )
}

internal fun SegmentState.termAt(idx: Int): String {
    val start = termOffsets[idx]
    val end = if (idx + 1 == termOffsets.size) termData.size
    else termOffsets[idx + 1]

    return String(termData.copyOfRange(start, end))
}

internal fun SegmentState.find(term: String): List<SearchResultEntry<Int>> {
    val termId = binarySearch(0, termOffsets.size - 1) {
        termAt(it) >= term
    }

    if (termId < 0 || termAt(termId) != term) return listOf()

    var i = binarySearch(0, dataTermIds.size - 1) {
        dataTermIds[it] >= termId
    }

    val result = mutableListOf<SearchResultEntry<Int>>()
    while (i in dataTermIds.indices) {
        if (!documentsState[dataDocIds[i]]) {
            i++
            continue
        }
        val currentTermId = dataTermIds[i]
        val current = termAt(currentTermId)
        if (current != term) break
        result += SearchResultEntry(
            documentName = documents[dataDocIds[i]],
            term = current,
            termData = dataTermData[i],
        )
        i++
    }

    return result
}

private fun binarySearch(start: Int, end: Int, condition: (Int) -> Boolean): Int {
    if (end < start) return -1

    var l = start
    var r = end

    while (l < r) {
        val m = (l + r) / 2
        if (condition(m)) {
            r = m
        } else {
            l = m + 1
        }
    }

    return if (condition(l)) l else -1
}
