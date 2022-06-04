package me.blzr.vote

import java.io.InputStream
import java.util.stream.Stream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent


object StaxXmlStream {
    fun of(inputStream: InputStream): Stream<Vote> {
        val result: MutableList<Vote> = mutableListOf()

        val xmlInputFactory = XMLInputFactory.newInstance()
        val reader: XMLStreamReader = xmlInputFactory.createXMLStreamReader(inputStream)

        var eventType = reader.eventType
        var name = ""
        var birthDay = ""
        var station = ""
        var time = ""

        while (reader.hasNext()) {
            eventType = reader.next()

//            <voters>
//              <voter name="Иван Иванов" birthDay="1943.22.19">
//                  <visit station="121" time="2016.10.18 16:52:41" />
//              </voter>
//            ...
//            </voters>

            if (eventType == XMLEvent.START_ELEMENT) {
                when (reader.name.localPart) {
                    "voter" -> {
                        name = reader.getAttributeValue(null, "name")
                        birthDay = reader.getAttributeValue(null, "birthDay")
                    }
                    "visit" -> {
                        station = reader.getAttributeValue(null, "station")
                        time = reader.getAttributeValue(null, "time")
                    }
                }
            } else if (eventType == XMLEvent.END_ELEMENT) {
                when (reader.name.localPart) {
                    "voter" -> {
                        val vote = Vote.parse(name, birthDay, station, time)
                        result.add(vote)
                        name = ""
                        birthDay = ""
                        station = ""
                        time = ""
                    }
                }
            }
        }

        return result.stream()
    }
}
