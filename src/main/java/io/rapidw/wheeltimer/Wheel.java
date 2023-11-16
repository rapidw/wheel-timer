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
package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

class Wheel {
    private static final Logger logger = LoggerFactory.getLogger(Wheel.class);

    private final int level;
    private final int tickCount;
    // nanos
    private final long tickDuration;
    //    private final TimeUnit timeUnit;
    private final Bucket[] buckets;
    // base time of this wheel, will be updated when the wheel is empty and a new task is added
    private long baseTime; // millis

    // previous wheel, first wheel has no previous wheel
    private final Wheel prev;


    public Wheel(int tickCount, long tickDuration, long baseTime, Wheel prev) {
        this.tickCount = tickCount;
        this.tickDuration = tickDuration;
//        this.timeUnit = timeUnit;
        this.baseTime = baseTime;
        this.buckets = new Bucket[tickCount];
        this.prev = prev;
        this.level = (prev == null ? 1 : prev.getLevel() + 1);
        logger.debug("new wheel, level {}, tick duration={}, tickCount={}, baseTime={}", level, tickDuration, tickCount, Formatter.formatInstant(baseTime));
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = new Bucket(this, i * tickDuration);
        }
    }

    public Bucket addTask(TimerTaskHandle handle) {
        // if wheel is empty, update baseTime
        if (this.isEmpty()) {
            logger.debug("update baseTime, old baseTime {}", Formatter.formatInstant(this.baseTime));
            // new baseTime should be integral times of tickDuration plus old baseTime
            this.baseTime = computeTime(this.baseTime, handle.getDeadline(), this.tickCount, this.tickDuration);
        }
        // find the bucket to add
        var no = (handle.getDeadline() - this.baseTime) / tickDuration % tickCount;
        logger.debug("add task, deadline={}, wheel level={},  baseTime={}", Formatter.formatInstant(handle.getDeadline()), this.level, Formatter.formatInstant(this.baseTime));
        logger.debug("add task to bucket no={}", no);
        Bucket bucket = this.buckets[(int) no];
        bucket.add(handle);
        handle.setBucket(bucket);
        return bucket;
    }

    /**
     * if the wheel is empty, find the new base time, otherwise return the current base time
     *
     * @param target deadline of the task
     * @return base time
     */
    long findNewBaseTime(long target) {
        if (isEmpty()) {
            return computeTime(this.baseTime, target, this.tickCount, this.tickDuration);
        } else {
            return this.baseTime;
        }
    }

    public long getTotalDuration() {
        return tickCount * tickDuration;
    }

    public String toString() {
        return "Wheel:level=" + level + ", tickCount=" + tickCount + ", tickDuration=" + tickDuration;
    }

    boolean isEmpty() {
        return Arrays.stream(buckets).allMatch(Bucket::isEmpty);
    }

    public Optional<Bucket> findFirstBucket() {
        return Arrays.stream(buckets).filter(bucket -> !bucket.isEmpty()).findFirst();
    }

    public List<TimerTaskHandle> getAllTasks() {
        return Arrays.stream(buckets).flatMap(bucket -> bucket.getHandles().stream()).toList();
    }

    int getLevel() {
        return level;
    }

    long getTickDuration() {
        return tickDuration;
    }

//    ChronoUnit getTimeUnit() {
//        return timeUnit;
//    }

    long getBaseTime() {
        return baseTime;
    }

    void setBaseTime(long baseTime) {
        this.baseTime = baseTime;
    }

    Wheel getPrev() {
        return prev;
    }

    public static long computeTime(long base, long target, int tickCount, long tickDuration) {
        return base + (target - base) / tickDuration / tickCount * tickCount * tickDuration;
//        return base.plus(Duration.between(base, target).get(timeUnit) / tickDuration / tickCount * tickCount * tickDuration, timeUnit);
    }
}
