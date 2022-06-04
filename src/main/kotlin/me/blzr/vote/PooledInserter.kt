package me.blzr.vote

import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.function.Function
import javax.sql.DataSource

class PooledInserter(private val dataSource: DataSource, executors: Int) : Function<Collection<Vote>, Future<*>> {
    // We want thread pool die with application
    private val executorPool = Executors.newFixedThreadPool(executors, ThreadFactoryBuilder().setDaemon(true).build())

    override fun apply(t: Collection<Vote>): Future<*> =
        executorPool.submit {
            dataSource.connection.use { connection ->
                scheduleInsert(connection, t)
            }
        }

    private fun scheduleInsert(connection: Connection, votes: Collection<Vote>): IntArray =
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
