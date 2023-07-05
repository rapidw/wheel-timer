package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

class Wheel {
    private static final Logger logger = LoggerFactory.getLogger(Wheel.class);
    
    private final int level;
    private final int tickCount;
    private final int tickDuration;
    private final ChronoUnit timeUnit;
    private final Bucket[] buckets;
    // base time of this wheel, will be updated when the wheel is empty and a new task is added
    private Instant baseTime; // millis

    // previous wheel, first wheel has no previous wheel
    private final Wheel prev;

    
    public Wheel(int tickCount, int tickDuration, ChronoUnit timeUnit, Instant baseTime, Wheel prev) {
        this.tickCount = tickCount;
        this.tickDuration = tickDuration;
        this.timeUnit = timeUnit;
        this.baseTime = baseTime;
        this.buckets = new Bucket[tickCount];
        this.prev = prev;
        this.level = prev == null ? 1 : prev.getLevel() + 1;
        logger.debug("new wheel, tick duration={}, tickCount={}, baseTime={}", tickDuration, tickCount, Formatter.formatInstant(baseTime));
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = new Bucket(this, i * tickDuration);
        }
    }

    public Bucket addTask(TimerTaskHandle handle) {
        // 如果当前轮是空的，则修改baseTime
        if (this.isEmpty()) {
            logger.debug("update baseTime, old baseTime {}", Formatter.formatInstant(this.baseTime));
            // new baseTime should be integral times of tickDuration plus old baseTime
            this.baseTime = this.baseTime.plus(Duration.between(this.baseTime, handle.getDeadline()).get(timeUnit) / tickDuration / tickCount * tickCount * tickDuration, timeUnit);
        }
        // 找到bucket并放入
        var no = Duration.between(this.baseTime, handle.getDeadline()).get(timeUnit) / tickDuration % tickCount;
        logger.debug("add task, deadline={}, wheel level={},  baseTime={}", Formatter.formatInstant(handle.getDeadline()), this.level, Formatter.formatInstant(this.baseTime));
        logger.debug("add task to bucket no={}", no);
        Bucket bucket = this.buckets[(int)no];
        bucket.add(handle);
        handle.setBucket(bucket);
        return bucket;
    }

    Instant findNewBaseTime(Instant instant) {
        if (isEmpty()) {
            return this.baseTime.plus(Duration.between(this.baseTime, instant).get(timeUnit) / tickDuration / tickCount * tickCount * tickDuration, timeUnit);
        } else {
            return this.baseTime;
        }
    }

    public int getTotalDuration() {
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

    int getTickDuration() {
        return tickDuration;
    }

    ChronoUnit getTimeUnit() {
        return timeUnit;
    }

    Instant getBaseTime() {
        return baseTime;
    }

    void setBaseTime(Instant baseTime) {
        this.baseTime = baseTime;
    }

    Wheel getPrev() {
        return prev;
    }
}
