package me.blzr.vote

import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import java.io.InputStream
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import javax.xml.parsers.DocumentBuilderFactory
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

    private fun parseDomXml(input: InputStream): Document? {
        val documentBuilderFactory = DocumentBuilderFactory.newInstance()
        val documentBuilder = documentBuilderFactory.newDocumentBuilder()
        return documentBuilder.parse(input)
    }

    private fun parseVoter(voter: Node): Vote {
        val name = voter.attributes.getNamedItem("name").textContent
        val birthDay = voter.attributes.getNamedItem("birthDay").textContent
        val visit = (voter as Element).getElementsByTagName("visit").item(0)
        val station = visit.attributes.getNamedItem("station").textContent
        val time = visit.attributes.getNamedItem("time").textContent

        return Vote.parse(name, birthDay, station, time)
    }

    private fun truncate(connection: Connection) {
        timeIt("cleanup") {
            connection.prepareStatement("TRUNCATE TABLE vote").use {
                it.execute()
            }
        }
    }

    private fun insert(connection: Connection, vote: Vote) {
        connection.prepareStatement("INSERT INTO vote (name, birthDay, station, time) VALUES(?, ?, ?, ?)").use {
            it.setString(1, vote.name)
            it.setDate(2, Date.valueOf(vote.birthDay))
            it.setInt(3, vote.station)
            it.setTimestamp(4, Timestamp.valueOf(vote.time))

            it.execute()
        }
    }

    fun loadDom(source: String) {
        dataSource.connection.use {
            truncate(it)
        }

        val resource = Application.javaClass.classLoader.getResource(source)
        if (resource == null) {
            println("File $source not found")
            exitProcess(0)
        }

        resource.openStream().use {
            val document = timeIt("Load") {
                parseDomXml(it)
            }

            val childNodes = document?.documentElement?.getElementsByTagName("voter")

            if (childNodes == null) {
                println("No child nodes")
            } else {
                println("Nodes ${childNodes.length}")
                timeIt("Iterate") {
                    var i = 0
                    for (voter: Node in childNodes) {
                        i++
                    }
                }
                dataSource.connection.use { connection ->
                    timeIt("Insert") {
                        for (voter: Node in childNodes) {
                            insert(connection, parseVoter(voter))
                        }
                    }
                }
            }
        }
    }
}

fun main() {
    print("Hello world")
    Application.init("jdbc:mysql://localhost:3306/db", "root", "pass")
    Application.loadDom("data-0.2M.xml")

}
