package io.rapidw.wheeltimer.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Formatter {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static String formatInstant(Instant instant) {
        return formatter.format(instant.atZone(ZoneOffset.ofHours(8)));
    }

    public static String formatDuration(Duration duration) {
        long days = duration.toDays();
        duration = duration.minusDays(days);
        long hours = duration.toHours();
        duration = duration.minusHours(hours);
        long minutes = duration.toMinutes();
        duration = duration.minusMinutes(minutes);
        long seconds = duration.toSeconds();
        duration = duration.minusSeconds(seconds);
        long millis = duration.toMillis();
        return (days == 0 ? "" : days + " days, ") + (hours == 0 ? "" : hours + " hours, ") +
                (minutes == 0 ? "" : minutes + " minutes, ") + (seconds == 0 ? "" : seconds + " seconds, ") +
                (millis == 0 ? "" : millis + " millis");
    }
}
