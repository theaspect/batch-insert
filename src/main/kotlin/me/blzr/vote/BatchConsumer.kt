package me.blzr.vote

import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.util.function.Consumer
import javax.sql.DataSource

class BatchConsumer(private val dataSource: DataSource, private val batchSize: Int) : Consumer<Vote> {
    private val bucket = ArrayList<Vote>(batchSize)

    override fun accept(vote: Vote) {
        bucket.add(vote)

        if (bucket.size >= batchSize) {
            dataSource.connection.use(this::doCommit)
            bucket.clear()
        }
    }

    private fun doCommit(connection: Connection) {
        connection.prepareStatement(
            "INSERT INTO vote (name, birthDay, station, time) VALUES(?, ?, ?, ?)"
        ).use { statement ->
            for (v in bucket) {
                statement.setString(1, v.name)
                statement.setDate(2, Date.valueOf(v.birthDay))
                statement.setInt(3, v.station)
                statement.setTimestamp(4, Timestamp.valueOf(v.time))

                statement.addBatch()
            }
            statement.executeBatch()
        }
    }
}
