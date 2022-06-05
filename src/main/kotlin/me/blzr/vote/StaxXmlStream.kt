package me.blzr.vote

import java.io.InputStream
import java.util.Spliterators.AbstractSpliterator
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.stream.Stream
import java.util.stream.StreamSupport
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent


class StaxXmlStream(val inputStream: InputStream, queueSize: Int) : Thread("Producer") {
    val queue = ArrayBlockingQueue<Vote>(queueSize)
    var finished = false
    val read = AtomicLong(0)
    val consumed = AtomicLong(0)

    fun getStream(): Stream<Vote> =
        StreamSupport.stream(object : AbstractSpliterator<Vote>(Long.MAX_VALUE, CONCURRENT or NONNULL or ORDERED) {
            override fun tryAdvance(action: Consumer<in Vote>): Boolean {
                if (finished && queue.isEmpty()) {
                    // println("Finished")
                    return false
                }

                consumed.incrementAndGet()
                action.accept(queue.take())
                return true
            }
        }, false)

    override fun run() {
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
                        read.incrementAndGet()
                        // This is just for logging purposes
                        if (!queue.offer(vote)) {
                            // println("Queue is full, waiting consumer")
                            queue.put(vote)
                        }

                        name = ""
                        birthDay = ""
                        station = ""
                        time = ""
                    }
                }
            }
        }

        finished = true
    }
}
