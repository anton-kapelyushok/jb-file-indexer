package home.pathfinder.indexing

import java.io.File
import java.nio.file.Path
import java.util.*

internal val Path.canonicalPath: String get() = toFile().canonicalPath

private fun String.asPathString(): String = this + File.separator
private fun String.asString(): String = this.substring(0 until this.length - 1)

internal fun minimalRootsTree(roots: Iterable<String>): PathTree {
    val minimalRoots = PathTree()
    for (root in (roots)) {
        if (root in minimalRoots) continue
        if (minimalRoots.containsParentOf(root)) continue

        while (minimalRoots.anyChildOf(root) != null) {
            minimalRoots -= minimalRoots.anyChildOf(root)!!
        }

        minimalRoots += root
    }

    return minimalRoots
}

internal class PathTree() : Iterable<String> {
    private val treeSet = TreeSet<String>()

    constructor(paths: Iterable<String>) : this() {
        addAll(paths)
    }

    fun containsPathOrItsParent(path: String): Boolean {
        return pathOrItsParent(path) != null
    }

    fun pathOrItsParent(path: String): String? {
        if (path.asPathString() in treeSet) return path
        return parentOf(path)
    }

    fun containsParentOf(path: String): Boolean {
        return parentOf(path) != null
    }

    fun parentOf(path: String): String? {
        val parts = path.split(File.separator)
        for (i in parts.size - 1 downTo 0) {
            val parent = parts.subList(0, i).joinToString(File.separator)
            if ((parent + File.separator) in treeSet) return parent
        }
        return null
    }

    fun anyChildOf(path: String): String? {
        val pathString = path.asPathString()
        val closestChild = treeSet.higher(pathString)
        if (closestChild != null && closestChild.startsWith(pathString)) {
            return closestChild.asString()
        }
        return null
    }

    operator fun contains(path: String): Boolean {
        return path.asPathString() in treeSet
    }

    fun addAll(paths: Iterable<String>) {
        treeSet.addAll(paths.map { it.asPathString() })
    }

    override fun iterator(): Iterator<String> {
        return iterator {
            treeSet.asSequence().forEach { yield(it.asString()) }
        }
    }

    operator fun minusAssign(path: String) {
        treeSet -= path.asPathString()
    }

    operator fun plusAssign(path: String) {
        treeSet += path.asPathString()
    }
}
