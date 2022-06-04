package me.blzr.vote

import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import javax.sql.DataSource

class PooledInserter(private val dataSource: DataSource, executors: Int) : Consumer<Collection<Vote>> {
    private val executorPool = Executors.newFixedThreadPool(executors)

    override fun accept(t: Collection<Vote>) {
        executorPool.submit {
            dataSource.connection.use { connection ->
                scheduleInsert(connection, t)
            }
        }
    }

    private fun scheduleInsert(connection: Connection, votes: Collection<Vote>) {
        connection.prepareStatement(
            "INSERT INTO vote (name, birthDay, station, time) VALUES(?, ?, ?, ?)"
        ).use { statement ->
            for (v in votes) {
                statement.setString(1, v.name)
                statement.setDate(2, Date.valueOf(v.birthDay))
                statement.setInt(3, v.station)
                statement.setTimestamp(4, Timestamp.valueOf(v.time))

                statement.addBatch()
            }
            statement.executeBatch()
        }
    }

    fun await() {
        executorPool.shutdown()
        executorPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)
    }
}
