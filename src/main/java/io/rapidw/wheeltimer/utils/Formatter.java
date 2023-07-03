package io.rapidw.wheeltimer.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Formatter {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static String formatInstant(Instant instant) {
        return formatter.format(instant.atZone(ZoneOffset.ofHours(8)));
    }

    public static String formatDuration(Duration duration) {
        return duration.getSeconds() + "s";
    }
}
