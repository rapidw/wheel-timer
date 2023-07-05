package io.rapidw.wheeltimer;

import java.time.Instant;

public class TimerTaskHandle {

    private final TimerTask task;
    private final Instant deadline;
    private final Timer timer;
    private Bucket bucket;
    private volatile boolean canceled = false;
    private volatile boolean expired = false;

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

    TimerTask getTask() {
        return task;
    }

    public Instant getDeadline() {
        return deadline;
    }

    Bucket getBucket() {
        return bucket;
    }

    void setBucket(Bucket bucket) {
        this.bucket = bucket;
    }
}
