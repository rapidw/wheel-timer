package io.rapidw.wheeltimer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Slf4j
class Bucket implements Delayed, Iterable<TimerTaskHandle> {

    private final LinkedList<TimerTaskHandle> handles = new LinkedList<>();
    @Getter
    private final Wheel wheel;
    private final long delay;

    public Bucket(Wheel wheel, long delay) {
        this.wheel = wheel;
        this.delay = delay;
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

    @Override
    public long getDelay(TimeUnit unit) {
        long duration = wheel.getBaseTime() + delay - System.currentTimeMillis();
        return unit.convert(duration, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        log.debug("compare called");
        if (this == o) {
            log.debug("return equal");
            return 0;
        }
        long delay1 = getDelay(TimeUnit.MILLISECONDS);
        long delay2 = o.getDelay(TimeUnit.MILLISECONDS);
        log.debug("delay1={}, delay2={}", delay1, delay2);
        val res = delay1 - delay2;
        if (res > 0) {
            return 1;
        } else if (res == 0) {
            return 0;
        } else {
            return -1;
        }
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
