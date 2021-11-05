package home.pathfinder.indexing.segmentedindex

import assertk.assertThat
import assertk.assertions.containsAll
import assertk.assertions.isEmpty
import home.pathfinder.indexing.Posting
import home.pathfinder.indexing.SearchResultEntry
import org.junit.jupiter.api.Test

class SegmentStateTest {
    @Test
    fun `should be able to search on segment`() {

        val segment1 = createSegment(
            "doc1",
            "Приходит актёр в провинциальный театр устраиваться на работу. А там ему говорят".split(" ")
                .map { Posting(it, 1) } +
                    "Волобуев! Вот Ваш меч".split(" ").map { Posting(it, 2) }
        )

        val segment2 = createSegment(
            "doc2",
            "Подходит Петька к Василиванычу и спрашивает".split(" ").map { Posting(it, 1) } +
                    "-Василиваныч что такое НЮАНС".split(" ").map { Posting(it, 2) } +
                    "Василивааныч и говорит".split(" ").map { Posting(it, 3) } +
                    "-снимай Петька штаны".split(" ").map { Posting(it, 4) }
        )

        val segment3 = mergeSegments(segment1, segment2)

        val segment4 = segment3.deleteDocument("doc1")
        val segment5 = createSegment(
            "doc1",
            "Пупа и Лупа пришли в парфюмерную".split(" ").map { Posting(it, 1) } +
                    "Им выдали пробники духов, но у Лупы был заложен нос".split(" ").map { Posting(it, 2) }
        )
        val segment6 = mergeSegments(segment4, segment5)

        val segment7 = createSegment("doc3",
            "Два последних диктатора Европы - Пупа и Лупа".split(" ").map { Posting(it, 1) } +
                    "И знают, что последние, поэтому так отчаянно держатся друг за друга".split(" ")
                        .map { Posting(it, 2) } +
                    "Лупа за Пупу".split(" ").map { Posting(it, 3) }
        )

        val segment8 = mergeSegments(segment6, segment7)
        val segment9 = segment8.deleteDocument("doc2")

        assertThat(segment9.find("Пупа")).containsAll(
            SearchResultEntry("doc3", "Пупа", 1),
            SearchResultEntry("doc1", "Пупа", 1),
        )

        assertThat(segment9.find("Волобуев")).isEmpty()
        assertThat(segment9.find("Петька")).isEmpty()
    }
}