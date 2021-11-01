package home.pathfinder.indexing.segmentedindex

import home.pathfinder.indexing.Posting
import home.pathfinder.indexing.SearchResultEntry

@Suppress("ArrayInDataClass")
data class SegmentState(
    val docNames: Array<String>,
    val docStates: BooleanArray,
    val terms: Array<String>,

    val termArray: IntArray,
    val docArray: IntArray,
    val termDataArray: IntArray,

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

fun createSegment(documentName: String, documentData: List<Posting<Int>>): SegmentState {
    val documentNames = arrayOf(documentName)
    val documentStates = booleanArrayOf(true)
    val terms = documentData.map { it.term }.sorted().distinct().toTypedArray()

    val termArray = IntArray(documentData.size)
    val docArray = IntArray(documentData.size)
    val termDataArray = IntArray(documentData.size)

    val postingsPerDocument = intArrayOf(documentData.size)
    val alivePostings = documentData.size

    documentData
        .sortedBy { it.term }
        .forEachIndexed { idx, (term, termData) ->
            val termV = terms.binarySearch(term)
            termArray[idx] = termV
            docArray[idx] = 0
            termDataArray[idx] = termData
        }

    return SegmentState(
        docNames = documentNames,
        docStates = documentStates,
        terms = terms,
        termArray = termArray,
        docArray = docArray,
        termDataArray = termDataArray,
        postingsPerDocument = postingsPerDocument,
        alivePostings = alivePostings
    )
}

fun deleteDocument(segment: SegmentState, documentName: String): SegmentState {
    val docIdx = segment.docNames.binarySearch(documentName)
    return segment.copy(
        docStates = segment.docStates.copyOf().also { it[docIdx] = false },
        alivePostings = segment.alivePostings - segment.postingsPerDocument[docIdx]
    )
}

fun mergeSegments(lSegment: SegmentState, rSegment: SegmentState): SegmentState {
    val aliveDocIndicesLeft = lSegment.docNames.indices.filter { lSegment.docStates[it] }
    val aliveDocIndicesRight = rSegment.docNames.indices.filter { rSegment.docStates[it] }
    val newDocNames =
        (aliveDocIndicesLeft.map { lSegment.docNames[it] } + aliveDocIndicesRight.map { rSegment.docNames[it] })
            .distinct()
            .sorted()
            .toTypedArray()

    val docNameMap = newDocNames.indices.associateBy { newDocNames[it] }

    val newDocStates = BooleanArray(newDocNames.size) { true }

    val newAlivePostings = lSegment.alivePostings + rSegment.alivePostings

    val newTermArray = IntArray(newAlivePostings)
    val newDocArray = IntArray(newAlivePostings)
    val newTermDataArray = IntArray(newAlivePostings)

    val newTerms = mutableListOf<String>()

    val newPostingsPerDocument = IntArray(newDocNames.size)

    run {
        var r = 0
        var l = 0

        var last = 0

        fun addFrom(segment: SegmentState, i: Int) {
            val term = segment.terms[segment.termArray[i]]
            if (newTerms.isEmpty() || term != newTerms.last()) {
                newTerms.add(term)
            }
            val termId = newTerms.size - 1
            newTermArray[last] = termId

            val docName = segment.docNames[segment.docArray[i]]
            val docId = docNameMap[docName]!!

            newDocArray[last] = docId

            newPostingsPerDocument[docId]++

            newTermDataArray[last] = segment.termDataArray[i]

            last++
        }

        while (l != lSegment.termArray.size || r != rSegment.termArray.size) {
            when {
                l in lSegment.termArray.indices && !lSegment.docStates[lSegment.docArray[l]] -> {
                    l++
                }
                r in rSegment.termArray.indices && !rSegment.docStates[rSegment.docArray[r]] -> {
                    r++
                }
                l !in lSegment.termArray.indices -> {
                    addFrom(rSegment, r)
                    r++
                }
                r !in rSegment.termArray.indices -> {
                    addFrom(lSegment, l)
                    l++
                }
                else -> {
                    val lTerm = lSegment.terms[lSegment.termArray[l]]
                    val rTerm = rSegment.terms[rSegment.termArray[r]]
                    if (lTerm < rTerm) {
                        addFrom(lSegment, l)
                        l++
                    } else {
                        addFrom(rSegment, r)
                        r++
                    }
                }
            }
        }

        return SegmentState(
            docNames = newDocNames,
            docStates = newDocStates,
            terms = newTerms.toTypedArray(),
            termArray = newTermArray,
            docArray = newDocArray,
            termDataArray = newTermDataArray,
            postingsPerDocument = newPostingsPerDocument,
            alivePostings = newAlivePostings
        )
    }
}

fun getAliveDocuments(segment: SegmentState): List<String> {
    return segment.docNames.indices.filter { segment.docStates[it] }.map { segment.docNames[it] }
}

fun findInSegment(segment: SegmentState, term: String): List<SearchResultEntry<Int>> {
    val termId = segment.terms.binarySearch(term)
    if (termId < 0) return listOf()

    var i = segment.termArray.binarySearch(termId)
    while (i - 1 in segment.termArray.indices && segment.termArray[i - 1] == termId) i--

    val result = mutableListOf<SearchResultEntry<Int>>()

    while (i < segment.termArray.size) {
        if (!segment.docStates[segment.docArray[i]]) {
            i++
            continue
        }
        if (segment.termArray[i] != termId) {
            break
        }
        result.add(
            SearchResultEntry(
                documentName = segment.docNames[segment.docArray[i]],
                term = segment.terms[segment.termArray[i]],
                termData = segment.termDataArray[i]
            )
        )
        i++
    }

    return result
}

