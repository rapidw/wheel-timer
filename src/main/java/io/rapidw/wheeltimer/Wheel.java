package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.Formatter;
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

    @Getter
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
    private Instant baseTime; // millis

    // 前一个轮子，最前一个没有prev
    @Getter
    private final Wheel prev;

    @Builder
    public Wheel(int tickCount, int tickDuration, ChronoUnit timeUnit, Instant baseTime, Wheel prev) {
        this.tickCount = tickCount;
        this.tickDuration = tickDuration;
        this.timeUnit = timeUnit;
        this.baseTime = baseTime;
        this.buckets = new Bucket[tickCount];
        this.prev = prev;
        this.level = prev == null ? 1 : prev.getLevel() + 1;
        log.debug("new wheel, tick duration={}, tickCount={}, baseTime={}", tickDuration, tickCount, Formatter.formatInstant(baseTime));
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = new Bucket(this, i * tickDuration);
        }
    }

    public Bucket addTask(TimerTaskHandle handle) {
        // 如果当前轮是空的，则修改baseTime
        if (this.isEmpty()) {
            log.debug("update baseTime");
            this.baseTime = this.baseTime.plus(Duration.between(this.baseTime, handle.getDeadline()).get(timeUnit) / tickDuration / tickCount * tickCount * tickDuration, timeUnit);
        }
        // 找到bucket并放入
        val no = Duration.between(this.baseTime, handle.getDeadline()).get(timeUnit) / tickDuration % tickCount;
        log.debug("add task, deadline={}, wheel level={},  baseTime={}", Formatter.formatInstant(handle.getDeadline()), this.level, Formatter.formatInstant(this.baseTime));
        log.debug("add task to bucket no={}", no);
        Bucket bucket = this.buckets[(int)no];
        bucket.add(handle);
        return bucket;
    }

    public int getTotalDuration() {
        return tickCount * tickDuration;
    }

    public String toString() {
        return "Wheel:level=" + level + ", tickCount=" + tickCount + ", tickDuration=" + tickDuration;
    }

    private boolean isEmpty() {
        for (Bucket bucket : buckets) {
            if (!bucket.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public Bucket findFirstBucket() {
        for (Bucket bucket : buckets) {
            if (!bucket.isEmpty()) {
                return bucket;
            }
        }
        return null;
    }
}
