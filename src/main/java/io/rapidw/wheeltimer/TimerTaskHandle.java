package io.rapidw.wheeltimer;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.sql.Time;
import java.time.Instant;

public class TimerTaskHandle {

    @Getter
    private final TimerTask task;
    @Getter
    private final Instant deadline;
    private final Timer timer;
    @Setter(lombok.AccessLevel.PACKAGE)
    @Getter(lombok.AccessLevel.PACKAGE)
    private Bucket bucket;
    private volatile boolean canceled = false;
    private volatile boolean expired = false;

    @Builder
    public TimerTaskHandle(Timer timer, TimerTask task, Instant deadline) {
        this.timer = timer;
        this.task = task;
        this.deadline = deadline;
    }

    public void cancel() {
        this.canceled = true;
        timer.cancelTask(this);
    }
    void setExpired() {
        this.expired = true;
    }
    public boolean isExpired() {
        return expired;
    }
    public boolean isCancelled() {
        return canceled;
    }
}
