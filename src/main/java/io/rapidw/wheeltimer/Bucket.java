package io.rapidw.wheeltimer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

class Bucket {
    private static final Logger logger = LoggerFactory.getLogger(Bucket.class);

    private final List<TimerTaskHandle> handles = new LinkedList<>();
    private final Wheel wheel;
    private final int deadlineOffset;

    public Bucket(Wheel wheel, int deadlineOffset) {
        this.wheel = wheel;
        this.deadlineOffset = deadlineOffset;
    }
    void add(TimerTaskHandle handle) {
        handles.add(handle);
    }

    int runAndClearTasks(Executor executor) {
        AtomicInteger i = new AtomicInteger();
        handles.forEach(handle ->
        {
            if (!handle.isCancelled()) {
                handle.setExpired();
                executor.execute(() -> handle.getTask().run(handle));
                i.getAndIncrement();
            } else {
                logger.debug("[bug] canceled task run");
            }
        });
        handles.clear();
        return i.get();
    }

    Instant getDeadline() {
        return wheel.getBaseTime().plus(Duration.of(deadlineOffset, wheel.getTimeUnit()));
    }

    boolean isEmpty() {
        return handles.isEmpty();
    }

    void clear() {
        handles.clear();
    }

    void remove(TimerTaskHandle handle) {
        handles.remove(handle);
    }

    List<TimerTaskHandle> getHandles() {
        return handles;
    }

    Wheel getWheel() {
        return wheel;
    }
}
