package me.blzr.vote

import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.text.DecimalFormat

fun <T> timeIt(message: String = "", callback: () -> T): T{
    val start = System.nanoTime()
    val result = callback.invoke()
    val delta = DecimalFormat(".2ms").format((System.nanoTime() - start)/1_000_000)
    println("$message $delta")
    return result
}

operator fun NodeList.iterator(): Iterator<Node> = object:Iterator<Node>{
        private var index = 0

        override fun hasNext(): Boolean =
            index < this@iterator.length

        override fun next(): Node {
            if (!hasNext()) throw NoSuchElementException()
            return this@iterator.item(index++)
        }
    }
