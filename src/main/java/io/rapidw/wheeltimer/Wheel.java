package io.rapidw.wheeltimer;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.concurrent.TimeUnit;

@Slf4j
class Wheel {

    @Getter
    private final int tickCount;
    @Getter
    private final int tickDuration;
    @Getter
    private final TimeUnit timeUnit;
    private final Bucket[] buckets;
    // 本轮的开始时间
    @Getter
    @Setter
    private volatile long baseTime;
    @Getter
    private final Wheel prev;

    @Builder
    public Wheel(int tickCount, int tickDuration, TimeUnit timeUnit, long baseTime, Wheel prev) {
        this.tickCount = tickCount;
        this.tickDuration = tickDuration;
        this.timeUnit = timeUnit;
        this.baseTime = baseTime;
        this.buckets = new Bucket[tickCount];
        this.prev = prev;
        log.debug("new wheel, tick duration={}", tickDuration);
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = new Bucket(this, timeUnit.toNanos(tickDuration) * (i + 1));
        }
    }

    public Bucket addTask(TimerTaskHandle handle) {
        // 找到bucket并放入
        val delayNanos = handle.getDeadline() - this.baseTime;
        log.debug("add task, delay={}, wheel duration={}", TimeUnit.SECONDS.convert(delayNanos, TimeUnit.NANOSECONDS), TimeUnit.SECONDS.convert(tickDuration, timeUnit));
        val no = delayNanos / timeUnit.toNanos(tickDuration) % tickCount - 1;
        log.debug("add task to bucket no={}", no);
        Bucket bucket = this.buckets[(int)no];
        bucket.add(handle);
        return bucket;
    }

    public String toString() {
        return String.format("轮子tickCount:%d, tickDuration:%d, baseTime:%d", tickCount, tickDuration, baseTime);
    }
}
