package me.blzr.vote

import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import java.net.URL
import javax.sql.DataSource
import kotlin.system.exitProcess

object Application {
    lateinit var dataSource: HikariDataSource

    fun init(url: String, user: String, pass: String) {
        val flyway: Flyway = Flyway.configure().dataSource(url, user, pass).load()
        flyway.migrate()

        dataSource = with(HikariDataSource()) {
            jdbcUrl = url
            username = user
            password = pass
            return@with this
        }
    }


    private fun truncate(dataSource: DataSource) {
        timeIt("cleanup") {
            dataSource.connection.use { connection ->
                connection.prepareStatement("TRUNCATE TABLE vote").use {
                    it.execute()
                }
            }
        }
    }

    fun loadDom(source: String) {
        val resource = getResource(source)

        println("Just Parse via DOM XML")
        resource.openStream().use { input ->
            truncate(dataSource)
            Parser.runTimed(input) {/* No OP */ }
        }

        listOf(1, 2, 3).forEach { jobs ->
            listOf(1, 10, 20, 50, 100, 200, 500, 1000, 2000).forEach { batch ->
                timeIt("XML Parse for $batch in $jobs") {
                    val parallelConsumer = PooledInserter(dataSource, jobs)
                    val batchingConsumer = BatchingConsumer(batch, parallelConsumer)
                    truncate(dataSource)

                    resource.openStream().use { input ->
                        Parser.run(input, batchingConsumer)
                        parallelConsumer.await()
                    }
                }
            }
        }
    }

    private fun getResource(classpath: String): URL {
        val resource = Application.javaClass.classLoader.getResource(classpath)
        if (resource == null) {
            println("File $classpath not found")
            exitProcess(0)
        }
        return resource
    }
}

fun main() {
    print("Hello world")
    Application.init("jdbc:mysql://localhost:3306/db", "root", "pass")
    Application.loadDom("data-0.2M.xml")

}
