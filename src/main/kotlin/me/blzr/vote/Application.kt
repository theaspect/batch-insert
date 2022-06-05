package me.blzr.vote

import com.google.common.collect.Sets
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import java.net.URL
import java.util.function.Function


object Application {
    lateinit var dataSource: HikariDataSource

    fun init(url: String, user: String, pass: String) {
        dataSource = with(HikariDataSource()) {
            jdbcUrl = url
            username = user
            password = pass
            return@with this
        }

        Flyway.configure().dataSource(dataSource).load().migrate()
    }

    fun run(source: String) {
        val resource = getResource(source)

        // domDryRun(resource)
        // staxDryRun(resource)

        println("StAX parser")
        Sets.cartesianProduct(
            // Jobs
            setOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
            // Chunk size
            setOf(1, 10, 20, 50, 100, 200, 500, 1000, 2000)
        ).forEach { (jobs, chunkSize) ->
            truncate(dataSource)
            timeIt("$chunkSize\t$jobs\t") {
                resource.openStream().use { input ->
                    val reader = StaxXmlStream(input, jobs * chunkSize)
                    val consumer = PooledInserter(dataSource, jobs)

                    reader.start()
                    reader.getStream()
                        .chunked(chunkSize)
                        .map(consumer::apply)
                        .forEach { it.get() }

                    //println("Wait for producer finish")
                    reader.join()
                    //println("Wait for consumer finish")
                    consumer.join()


                    val read = reader.read.get()
                    val consumed = reader.consumed.get()
                    val db = getCount(dataSource)

                    if (read != consumed || read != db) {
                        throw java.lang.AssertionError("$read != $consumed != $db")
                    }
                }
            }
        }
    }

    private fun staxDryRun(resource: URL) {
        timeIt("Just Parse via StAX XML") {
            resource.openStream().use { input ->
                StaxXmlStream(input, 1)
                    .getStream()
                    .toStreamEx()
                    .map(Function.identity())
                    .toList()
            }
        }
    }

    private fun domDryRun(resource: URL) {
        timeIt("Just Parse via DOM XML") {
            resource.openStream().use { input ->
                DomXmlStream.of(input)
                    .toStreamEx()
                    .map(Function.identity())
                    .toList()
            }
        }
    }
}


fun main() {
    print("Hello world")
    Application.init("jdbc:mysql://localhost:3306/db", "root", "pass")
    Application.run("data-0.2M.xml")
}
