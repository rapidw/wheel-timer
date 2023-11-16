/**
 * Copyright 2023 Rapidw
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    public static String formatInstant(long nanos) {
        return formatInstant(Instant.ofEpochSecond(nanos / 1000000000, nanos % 1000000000));
    }

    public static String formatDuration(long duration) {
        return formatDuration(Duration.ofNanos(duration));
    }
}
