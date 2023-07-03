package io.rapidw.wheeltimer;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Slf4j
class Bucket implements Iterable<TimerTaskHandle> {

    private final LinkedList<TimerTaskHandle> handles = new LinkedList<>();
    @Getter
    private final Wheel wheel;
    private final int deadlineOffset;

    public Bucket(Wheel wheel, int deadlineOffset) {
        this.wheel = wheel;
        this.deadlineOffset = deadlineOffset;
    }
    public void add(TimerTaskHandle handle) {
        handles.add(handle);
    }

    public void runAndClearTasks() {
        handles.forEach(handle ->
        {
            if (!handle.isCancelled()) {
                handle.setExpired();
                handle.getTask().run(false, false);
            }
        });
        handles.clear();
    }

    public Instant getDeadline() {
        return wheel.getBaseTime().plus(Duration.of(deadlineOffset, wheel.getTimeUnit()));
    }

    @Override
    public Iterator<TimerTaskHandle> iterator() {
        return handles.iterator();
    }

    public boolean isEmpty() {
        return handles.isEmpty();
    }

    public void clear() {
        handles.clear();
    }
}
