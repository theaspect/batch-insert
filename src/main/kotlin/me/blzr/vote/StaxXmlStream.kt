package me.blzr.vote

import java.io.InputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent

class StaxXmlStream(inputStream: InputStream) : Iterable<Vote> {
    val reader: XMLStreamReader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream)!!

    override fun iterator(): Iterator<Vote> = object : AbstractIterator<Vote>() {
        // var count = 0

        override fun computeNext() {
            var name = ""
            var birthDay = ""
            var station = ""
            var time = ""

            while (reader.hasNext()) {
                reader.next()

                /*
                <voters>
                  <voter name="Иван Иванов" birthDay="1943.22.19">
                      <visit station="121" time="2016.10.18 16:52:41" />
                  </voter>
                ...
                </voters>
                */

                when (reader.eventType) {
                    XMLEvent.START_ELEMENT -> {
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
                    }
                    XMLEvent.END_ELEMENT -> {
                        when (reader.name.localPart) {
                            "voter" -> {
                                val vote = Vote.parse(name, birthDay, station, time)
                                // Check if buffer full then send and clean
                                // otherwise do nothing
                                setNext(vote)
                                //count += 1
                                //if (count % 100 == 0) println("Read $count ${Thread.currentThread()}")
                                break
                            }
                            "voters" -> {
                                done()
                                break
                            }
                        }
                    }
                }
            }
        }
    }
}
