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

            val elapsed = measureTimeMillis {
                resource.openStream().use { inputStream ->
                    runBlocking {
                        runBenchmark(inputStream, jobs, chunkSize)
                    }
                }
            }

            val total = getCount(dataSource)
            println("$jobs\t$chunkSize\t$elapsed\t${total * 1000 / elapsed}")
        }
    }

    @ObsoleteCoroutinesApi
    @ExperimentalCoroutinesApi
    private suspend fun runBenchmark(inputStream: InputStream, jobs: Int, chunkSize: Int) = coroutineScope {
        val channel = Channel<Collection<Vote>>(chunkSize)
        launch {
            StaxXmlStream(inputStream)
                .asSequence()
                .chunked(chunkSize)
                .forEach {
                    //println("Sending ${it.size} ${Thread.currentThread()}")
                    channel.send(it)
                }
            channel.close()
            //println("Finished produce ${Thread.currentThread()}")
        }

        repeat(jobs) {
            launch(Dispatchers.IO) {
                consumeChannel(channel)
                //println("Finished consume $it ${Thread.currentThread()}")
            }
        }
    }

    private suspend fun consumeChannel(channel: Channel<Collection<Vote>>) {
        dataSource.connection.use {
            for (chunk in channel) {
                //println("Insert ${chunk.size} in ${Thread.currentThread()}")
                scheduleInsert(it, chunk)
            }
        }
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
