package home.pathfinder.indexing

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test
import java.io.File

class PathTreeTest {
    @Test
    fun `should find parent`() {
        val tree = PathTree()

        tree.addAll(
            listOf(
                "" / "loupa" / "poupa",
                "" / "loupa" / "poup",
                "" / "loupa",
            )
        )

        assertThat(tree.parentOf("" / "loupa" / "poupa")).isEqualTo("" / "loupa")
        assertThat(tree.parentOf("" / "loupa" / "volobuev")).isEqualTo("" / "loupa")
        assertThat(tree.parentOf("" / "loupa" / "poupa" / "mech")).isEqualTo("" / "loupa" / "poupa")
        assertThat(tree.parentOf("" / "loupa" / "poupapa")).isEqualTo("" / "loupa")
        assertThat(tree.parentOf("" / "poupa" / "loupa")).isEqualTo("" / "loupa")
    }

    @Test
    fun `should find child`() {
        val tree = PathTree()

        tree.addAll(
            listOf(
                "" / "loupa" / "poupa",
                "" / "loupa" / "poupapa",
                "" / "loupa" / "poup",
                "" / "loupa" / "poup" / "mech",
                "" / "loupa",
            )
        )

        assertThat(tree.anyChildOf("" / "loupa" / "poup")).isEqualTo("" / "loupa" / "poup" / "mech")
        assertThat(tree.anyChildOf("" / "loupa" / "poupa")).isEqualTo(null)
    }
}


infix operator fun String.div(s: String): String {
    return "$this${File.separator}$s"
}
