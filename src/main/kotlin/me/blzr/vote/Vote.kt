package me.blzr.vote

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

data class Vote(
    val name: String,
    val birthDay: LocalDate,
    val station: Int,
    val time: LocalDateTime
) {
    companion object {
        private val birthDayFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")
        private val timeFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm:ss")
        fun parse(name: String, birthDay: String, station: String, time: String) =
            Vote(
                name,
                LocalDate.parse(birthDay, birthDayFormatter),
                station.toInt(),
                LocalDateTime.parse(time, timeFormatter)
            )
    }
}
