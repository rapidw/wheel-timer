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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

class Bucket {
    private static final Logger logger = LoggerFactory.getLogger(Bucket.class);

    private final List<TimerTaskHandle> handles = new LinkedList<>();
    private final Wheel wheel;
    private final long deadlineOffset;

    public Bucket(Wheel wheel, long deadlineOffset) {
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

    long getDeadline() {
        return wheel.getBaseTime() + deadlineOffset;
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
