package me.blzr.vote

import com.google.common.collect.Sets
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
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

        timeIt("Just Parse via DOM XML") {
            resource.openStream().use { input ->
                DomXmlStream.of(input)
                    .toStreamEx()
                    .map(Function.identity())
                    .toList()
            }
        }

        timeIt("Just Parse via StAX XML") {
            resource.openStream().use { input ->
                StaxXmlStream.of(input)
                    .toStreamEx()
                    .map(Function.identity())
                    .toList()
            }
        }

        Sets.cartesianProduct(
            // Jobs
            // setOf(1, 2, 3, 4, 5, 6, 7, 8),
            setOf(7, 8, 9, 10, 11, 12),
            // Chunk size
            //setOf(1, 10, 20, 50, 100, 200, 500, 1000, 2000)
            setOf(50, 100, 200)
        ).forEach { (jobs, chunkSize) ->
            truncate(dataSource)
            timeIt("XML Parse for $chunkSize in $jobs") {
                resource.openStream().use { input ->
                    DomXmlStream.of(input)
                        .chunked(chunkSize)
                        .map(PooledInserter(dataSource, jobs))
                        .toList() // Real start-up point
                        .forEach { it.get() }
                }
            }
        }
    }
}


fun main() {
    print("Hello world")
    Application.init("jdbc:mysql://localhost:3306/db", "root", "pass")
    Application.run("data-0.2M.xml")
}
