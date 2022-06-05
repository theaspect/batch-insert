package me.blzr.vote

import com.google.common.collect.Sets
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.flywaydb.core.Flyway
import java.io.InputStream
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureTimeMillis

@ObsoleteCoroutinesApi
object Application {
    private lateinit var dataSource: HikariDataSource

    fun init(url: String, user: String, pass: String) {
        dataSource = with(HikariDataSource()) {
            jdbcUrl = url
            username = user
            password = pass
            return@with this
        }

        Flyway.configure().dataSource(dataSource).load().migrate()
    }

    @ExperimentalCoroutinesApi
    fun run(source: String) {
        val resource = getResource(source)

        Sets.cartesianProduct(
            // Jobs
            setOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
            // Chunk size
            setOf(1, 10, 20, 50, 100, 200, 500, 1000, 2000)
        ).forEach { (jobs, chunkSize) ->
            truncate(dataSource)

            var total = 0
            val elapsed = measureTimeMillis {
                resource.openStream().use { inputStream ->
                    runBlocking {
                        total = runBenchmark(inputStream, jobs, chunkSize)
                    }
                }
            }

            println("$jobs\t$chunkSize\t$elapsed\t${total * 1000 / elapsed}")
        }
    }

    @ObsoleteCoroutinesApi
    @ExperimentalCoroutinesApi
    private suspend fun runBenchmark(inputStream: InputStream, jobs: Int, chunkSize: Int) = coroutineScope {
        val counter = AtomicInteger(0)
        val start = System.currentTimeMillis()

        val channel = Channel<Collection<Vote>>(chunkSize)
        launch {
            StaxXmlStream.of(inputStream, channel, chunkSize)
        }
        val consumer = launch(Dispatchers.IO.limitedParallelism(jobs)) {
            repeat(jobs) {
                counter.addAndGet(consumeChannel(channel))
            }
        }

        consumer.join()
        return@coroutineScope counter.get()
    }

    private suspend fun consumeChannel(channel: Channel<Collection<Vote>>): Int {
        var counter: Int = 0
        for (chunk in channel) {
            dataSource.connection.use {
                // println("Insert in ${Thread.currentThread()}")
                scheduleInsert(it, chunk)
                counter += chunk.size
            }
        }
        return counter
    }

    private fun scheduleInsert(connection: Connection, votes: Collection<Vote>): IntArray =
        connection.prepareStatement(
            "INSERT INTO vote (name, birthDay, station, time) VALUES(?, ?, ?, ?)"
        ).use { statement ->
            for (v in votes) {
                statement.setString(1, v.name)
                statement.setDate(2, Date.valueOf(v.birthDay))
                statement.setInt(3, v.station)
                statement.setTimestamp(4, Timestamp.valueOf(v.time))

                statement.addBatch()
            }
            statement.executeBatch()
        }
}


@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
fun main() {
    print("Hello world")
    Application.init("jdbc:mysql://localhost:3306/db", "root", "pass")
    Application.run("data-0.2M.xml")
}
