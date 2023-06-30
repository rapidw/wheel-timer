package io.rapidw.wheeltimer;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;


public class TimerTaskHandle {

    @Getter
    private final TimerTask task;
    @Getter
    // nanos
    private final Instant deadline;
    private volatile boolean canceled = false;
    private volatile boolean expired = false;

    @Builder
    public TimerTaskHandle(TimerTask task, Instant deadline) {
        this.task = task;
        this.deadline = deadline;
    }
//    private Instant getDeadline() {
//        return Instant.ofEpochSecond(deadlineNanos / 1_000_000_000, deadlineNanos % 1_000_000_000);
//    }

    public void cancel() {
        this.canceled = true;
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
