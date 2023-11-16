/**
 * Copyright 2023 Rapidw
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rapidw.wheeltimer;

public class TimerTaskHandle {

    private final TimerTask task;
    private final long deadline;
    private final Timer timer;
    private Bucket bucket;
    private volatile boolean canceled = false;
    private volatile boolean expired = false;

    public TimerTaskHandle(Timer timer, TimerTask task, long deadline) {
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

    public long getDeadline() {
        return deadline;
    }

    Bucket getBucket() {
        return bucket;
    }

    void setBucket(Bucket bucket) {
        this.bucket = bucket;
    }
}
