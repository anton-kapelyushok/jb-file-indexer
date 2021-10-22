package home.pathfinder.indexing

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import java.io.File
import java.io.FileNotFoundException

fun splitBySpace(path: String): Flow<Posting<Int>> = flow {
    try {
        File(path).bufferedReader().use { br ->
            br.lineSequence().forEachIndexed { idx, line ->
                line
                    .split(Regex("\\s+"))
                    .map { it.trim() }
                    .filter { it.isNotBlank() }
                    .forEach {
                        emit(Posting(it, idx + 1))
                    }
            }
        }
    } catch (e: FileNotFoundException) {
        // ignore
    }
}.flowOn(Dispatchers.IO)

