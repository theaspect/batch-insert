package me.blzr.vote

import java.util.function.Consumer

class BatchingConsumer<T>(
    private val batchSize: Int, private val bucketConsumer: Consumer<Collection<T>>
) : Consumer<T> {
    private var bucket = ArrayList<T>(batchSize)

    override fun accept(item: T) {
        bucket.add(item)

        if (bucket.size >= batchSize) {
            bucketConsumer.accept(bucket)
            bucket = ArrayList<T>(batchSize)
        }
    }
}
