package io.rapidw.wheeltimer

import java.time.Instant
import java.time.temporal.ChronoUnit

class Utils {
     static boolean isBetween(Instant desired, Instant actural, ChronoUnit timeUnit) {
        return actural.isAfter(desired.minus(1, timeUnit)) && (actural.isBefore(desired) || actural == desired)
    }
}
