package me.blzr.vote

import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import java.io.InputStream
import java.util.function.Consumer
import javax.xml.parsers.DocumentBuilderFactory

object Parser {
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

    fun runTimed(inputStream: InputStream, supplier: Consumer<Vote>) {
        timeIt("DOM XML") {
            val nodeCount = run(inputStream, supplier)
            println("$nodeCount nodes")
        }
    }

    fun run(inputStream: InputStream, supplier: Consumer<Vote>): Int {
        val document = parseDomXml(inputStream)

        val childNodes = document?.documentElement?.getElementsByTagName("voter")

        if (childNodes == null) {
            println("No child nodes")
            return 0
        }

        for (voter: Node in childNodes) {
            supplier.accept(parseVoter(voter))
        }
        return childNodes.length
    }
}
