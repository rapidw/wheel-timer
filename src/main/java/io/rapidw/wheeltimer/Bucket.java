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
    private final Instant deadline;

    public Bucket(Wheel wheel, Instant deadline) {
        this.wheel = wheel;
        this.deadline = deadline;
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
        return deadline;
    }

    @Override
    public Iterator<TimerTaskHandle> iterator() {
        return handles.iterator();
    }

    @Override
    public String toString() {

        return super.toString();
    }
}
