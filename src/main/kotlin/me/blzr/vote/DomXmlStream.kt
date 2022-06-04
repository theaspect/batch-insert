package me.blzr.vote

import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import java.io.InputStream
import java.util.*
import java.util.stream.Stream
import java.util.stream.StreamSupport
import javax.xml.parsers.DocumentBuilderFactory

object DomXmlStream {
    private fun parseDomXml(input: InputStream): Document {
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

    fun of(inputStream: InputStream): Stream<Vote> {
        val document = parseDomXml(inputStream)
        val childNodes = document.documentElement.getElementsByTagName("voter").iterator()

        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(childNodes, Spliterator.IMMUTABLE),
            false
        ).map { parseVoter(it) }
    }
}
