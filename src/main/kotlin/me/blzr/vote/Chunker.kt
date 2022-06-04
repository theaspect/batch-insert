package me.blzr.vote

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiPredicate

class Chunker<T>(private val chunkSize: Int) : BiPredicate<T, T> {
    private val counter = AtomicInteger(0)
    override fun test(t: T, u: T) =
        counter.incrementAndGet() % chunkSize != 0
}
