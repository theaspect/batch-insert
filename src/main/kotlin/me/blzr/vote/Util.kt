package me.blzr.vote

import one.util.streamex.StreamEx
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.net.URL
import java.sql.PreparedStatement
import java.util.stream.Stream
import javax.sql.DataSource

fun <T> timeIt(message: String = "", callback: () -> T): T {
    val start = System.nanoTime()
    val result = callback.invoke()
    val delta = ((System.nanoTime() - start) / 1_000_000).toInt()
    println("$message $delta")
    return result
}

operator fun NodeList.iterator(): Iterator<Node> = object : Iterator<Node> {
    private var index = 0

    override fun hasNext(): Boolean =
        index < this@iterator.length

    override fun next(): Node {
        if (!hasNext()) throw NoSuchElementException()
        return this@iterator.item(index++)
    }
}

fun <T> Stream<T>.toStreamEx() = StreamEx.of(this)

fun <T> Stream<T>.chunked(chunkSize: Int) = this.toStreamEx().groupRuns(Chunker(chunkSize))

fun getResource(classpath: String): URL =
    Application.javaClass.classLoader.getResource(classpath)
        ?: throw IllegalStateException("File $classpath not found")

fun truncate(dataSource: DataSource) =
    dataSource.connection.use { connection ->
        connection.prepareStatement("TRUNCATE TABLE vote").use(PreparedStatement::execute)
    }

fun truncateTimed(dataSource: DataSource) =
    timeIt("cleanup") { truncate(dataSource) }

fun getCount(dataSource: DataSource): Long =
    dataSource.connection.use { connection ->
        connection.prepareStatement("select count(*) from vote").use { ps ->
            ps.executeQuery().use { rs ->
                rs.next()
                rs.getLong(1)
            }
        }
    }

fun getCountTimed(dataSource: DataSource): Long =
    timeIt("Count") { getCount(dataSource) }
