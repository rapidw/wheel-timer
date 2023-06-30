package io.rapidw.wheeltimer;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
class Wheel {

    private final int level;
    @Getter
    private final int tickCount;
    @Getter
    private final int tickDuration;
    @Getter
    private final ChronoUnit timeUnit;
    private final Bucket[] buckets;
    // 本轮的开始时间
    @Getter
    @Setter
    private volatile Instant baseTime; // millis

    // 前一个轮子，最前一个没有prev
    @Getter
    private final Wheel prev;

    @Builder
    public Wheel(int level, int tickCount, int tickDuration, ChronoUnit timeUnit, Instant baseTime, Wheel prev) {
        this.level = level;
        this.tickCount = tickCount;
        this.tickDuration = tickDuration;
        this.timeUnit = timeUnit;
        this.baseTime = baseTime;
        this.buckets = new Bucket[tickCount];
        this.prev = prev;
        log.debug("new wheel, tick duration={}", tickDuration);
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = new Bucket(this, baseTime.plus(Duration.of((long) i * tickDuration, timeUnit)));
        }
    }

    public Bucket addTask(TimerTaskHandle handle) {
        // 找到bucket并放入
//        val delayNanos = handle.getDeadline() - this.baseTime;
        val no = Duration.between(handle.getDeadline(), this.baseTime).get(timeUnit) / tickDuration % tickCount;
        log.debug("add task, deadline={}, wheel level={},  baseTime={}", handle.getDeadline(), this.level, this.baseTime);
        log.debug("add task to bucket no={}", no);
        Bucket bucket = this.buckets[(int)no];
        bucket.add(handle);
        return bucket;
    }

    public String toString() {
        return String.format("wheel tickCount:%d, tickDuration:%d, baseTime:%d", tickCount, tickDuration, baseTime);
    }

    public int getTotalDuration() {
        return tickCount * tickDuration;
    }
}
