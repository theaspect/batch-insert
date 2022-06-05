package me.blzr.vote

import kotlinx.coroutines.channels.Channel
import java.io.InputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent


object StaxXmlStream {
    // TODO try to rewrite as iterator with pre-loading and then wrap to channel
    suspend fun of(inputStream: InputStream, channel: Channel<Collection<Vote>>, chunk: Int) {
        val xmlInputFactory = XMLInputFactory.newInstance()
        val reader: XMLStreamReader = xmlInputFactory.createXMLStreamReader(inputStream)

        var eventType = reader.eventType
        var name = ""
        var birthDay = ""
        var station = ""
        var time = ""

        var buffer = ArrayList<Vote>(chunk)

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
                        // Check if buffer full then send and clean
                        // otherwise do nothing
                        buffer = addSendAndClean(vote, buffer, channel, chunk)
                        name = ""
                        birthDay = ""
                        station = ""
                        time = ""
                    }
                }
            }
        }
        // Send remaining part
        if (buffer.size > 0) channel.send(buffer)
        channel.close()
    }

    private suspend fun addSendAndClean(
        vote: Vote,
        buffer: ArrayList<Vote>,
        channel: Channel<Collection<Vote>>,
        chunk: Int
    ): ArrayList<Vote> {
        return if (buffer.size < chunk) {
            buffer.add(vote)
            buffer
        } else {
            buffer.add(vote)
            channel.send(buffer)
            return ArrayList<Vote>(chunk)
        }
    }
}
