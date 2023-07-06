package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Timer {
    private static final Logger logger = LoggerFactory.getLogger(Timer.class);

    private final int tickPerWheel;
    private final int tickDuration;
    private final ChronoUnit tickTimeUnit;

    private final Thread workerThread;
    private final Executor executor;

    // guarded by lock, thread safe
    private final LinkedList<Wheel> wheels = new LinkedList<>();
    // guarded by lock
    private Bucket firstBucket;

    private enum WorkerThreadStatus {
        INIT,
        START,
        SHUTDOWN,
    }

    // guarded by lock
    private WorkerThreadStatus workerThreadStatus;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    // thread safe queue, not guarded by lock
    private final Queue<TimerTaskHandle> cancelledHandles = new ConcurrentLinkedQueue<>();

    private Instant startTime;
    private final AtomicInteger allTaskCounter = new AtomicInteger(0);
    private final AtomicInteger scheduledTaskCounter = new AtomicInteger(0);
    private final AtomicInteger cancelledTaskCounter = new AtomicInteger(0);
    private final AtomicInteger executedTaskCounter = new AtomicInteger(0);

    public Timer(Executor executor, int tickPerWheel, int tickDuration, ChronoUnit tickTimeUnit) {
        this.workerThreadStatus = WorkerThreadStatus.INIT;
        this.executor = executor;
        this.workerThread = new Thread(new Worker(), "timer-worker");
        this.tickPerWheel = tickPerWheel;
        this.tickDuration = tickDuration;
        this.tickTimeUnit = tickTimeUnit;
    }

    /**
     * start timer. not mandatory, timer will start when first task is added. only useful when you want to align timer start time
     *
     * @param startTime start time of timer
     */
    public void start(Instant startTime) {
        ensureStarted(startTime);
    }

    private void ensureStarted(Instant startTime) {
        logger.debug("starting timer");
        switch (workerThreadStatus) {
            case INIT -> {
                workerThreadStatus = WorkerThreadStatus.START;
                if (startTime != null) {
                    this.startTime = startTime;
                } else {
                    this.startTime = Instant.now();
                }
                workerThread.start();
            }
            case START -> {
            }
            case SHUTDOWN -> throw new TimerException("cannot be started once stopped");
            default -> throw new TimerException("[bug] invarid worker thread state");
        }
    }

    /**
     * shutdown timer and return all uncanceled tasks
     *
     * @return all uncanceled tasks
     */
    public List<TimerTaskHandle> stop() {
        logger.debug("stopping timer");
        workerThreadStatus = WorkerThreadStatus.SHUTDOWN;
        workerThread.interrupt();
        try {
            workerThread.join();
        } catch (InterruptedException e) {
            throw new TimerException("[bug] interrupted when waiting for worker thread to stop");
        }
        return wheels.stream().flatMap(wheel -> wheel.getAllTasks().stream()).toList();
    }

    /**
     * add task to timer, delay is the duration from now. Task will be executed one tickDuration early at most.
     *
     * @param task  task to run
     * @param delay delay from now
     * @return handle to cancel task
     */
    public TimerTaskHandle addTask(TimerTask task, Duration delay) {
        return addTask(task, Instant.now().plus(delay));
    }

    /**
     * add task to timer, deadline is the instant to run task. Task will be executed one tickDuration early at most.
     *
     * @param task     task to run
     * @param deadline deadline to run task
     * @return handle to cancel task
     */
    public TimerTaskHandle addTask(TimerTask task, Instant deadline) {
        logger.debug("add task, deadline: {}", Formatter.formatInstant(deadline));
        // timer must be started before add task
        ensureStarted(null);

        var now = Instant.now();
        var handle = new TimerTaskHandle(this, task, deadline);
        if (deadline.isBefore(now)) {
            logger.debug("deadline is before now, execute immediately");
            executor.execute(() -> {
                handle.setExpired();
                handle.getTask().run(handle);
            });
            return handle;
        }

        lock.lock();
        try {
            if (wheels.isEmpty()) {
                logger.debug("no wheel");
                // no wheels, build one and add task
                var newWheel = buildWheel(null, now, deadline);
                this.firstBucket = newWheel.addTask(handle);
                condition.signal();
            } else {
                logger.debug("has wheels");
                // has wheels, find the right wheel to add task
                if (isAllWheelEmpty()) {
                    logger.debug("all wheel empty");
                    var targetWheel = wheels.stream().filter(wheel -> {

                        var baseTime = wheel.findNewBaseTime(now);
                        logger.debug("current wheel level {}, new baseTime {}", wheel.getLevel(), Formatter.formatInstant(baseTime));
                        var maxTime = baseTime.plus(wheel.getTotalDuration());
                        return deadline.isBefore(maxTime) && deadline.isAfter(baseTime);
                    }).findFirst();

                    targetWheel.ifPresentOrElse(wheel -> {
                        logger.debug("found wheel, level {}", wheel.getLevel());
                        this.firstBucket = wheel.addTask(handle);
                        condition.signal();
                    }, () -> {
                        logger.debug("not found wheel, build new wheel");
                        var baseTime = wheels.getLast().findNewBaseTime(now);
                        this.firstBucket = buildWheel(baseTime, now, deadline).addTask(handle);
                        condition.signal();
                    });
                } else {
                    var wheel = wheels.getLast();
                    var baseTime = wheel.findNewBaseTime(now);
                    var maxTime = baseTime.plus(wheel.getTotalDuration());
                    if (deadline.isAfter(maxTime)) {
                        // target deadline is after last wheel, need to build a new wheel
                        logger.debug("need new wheel, build it");
                        buildWheel(baseTime, now, deadline).addTask(handle);
                        // bucket of new task is always after firstBucket, no need to update firstBucket
                        condition.signal();
                    } else {
                        logger.debug("no need new wheel, add to existing wheel");
                        var iter = wheels.descendingIterator();
                        while (iter.hasNext()) {
                            wheel = iter.next();
                            baseTime = wheel.findNewBaseTime(now);
                            maxTime = baseTime.plus(wheel.getTotalDuration());
                            logger.debug("current wheel {}", wheel);
                            if (deadline.isAfter(baseTime) && deadline.isBefore(maxTime)) {
                                // deadline in this wheel
                                logger.debug("wheel found, add to it");
                                var bucket = wheel.addTask(handle);
                                if (bucket.getDeadline().isBefore(firstBucket.getDeadline())) {
                                    // bucket of new task is before firstBucket, set it as firstBucket
                                    this.firstBucket = bucket;
                                }
                                condition.signal();
                                break;
                            } else {
                                logger.debug("not this wheel, continue");
                            }
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        this.allTaskCounter.incrementAndGet();
        this.scheduledTaskCounter.incrementAndGet();
        return handle;
    }

    /**
     * build wheels. When lastWheelBaseTime is not null, build a wheel with baseTime of it. Otherwise, build a wheel with baseTime of startTime.
     *
     * @param lastWheelBaseTime base time of last wheel
     * @param now               current time
     * @param deadline          deadline of new task
     * @return last wheel of new wheels
     */
    private Wheel buildWheel(Instant lastWheelBaseTime, Instant now, Instant deadline) {
        logger.debug("building new wheels");
        Duration remaining;
        Wheel prev = null;
        Wheel current;
        int currentTickDuration;
        if (lastWheelBaseTime != null) {
            // has wheels
            remaining = Duration.between(lastWheelBaseTime, deadline);
            currentTickDuration = wheels.getLast().getTickDuration() * this.tickPerWheel;
        } else {
            // compute baseTime based on startTime and now
            var baseTime = Wheel.computeTime(this.startTime, now, this.tickPerWheel, this.tickDuration, this.tickTimeUnit);
            remaining = Duration.between(baseTime, deadline);
            currentTickDuration = this.tickDuration;
        }

        logger.debug("remaining {}, currentTickDuration {}", Formatter.formatDuration(remaining), currentTickDuration);
        do {
            // baseTime will be updated in addTask
            current = new Wheel(this.tickPerWheel, currentTickDuration, this.tickTimeUnit, this.startTime, prev);
            prev = current;
            wheels.addLast(prev);
            remaining = remaining.minus(current.getTotalDuration());
            currentTickDuration = currentTickDuration * this.tickPerWheel;
            logger.debug("new remaining {}, new currentTickDuration {}", Formatter.formatDuration(remaining), currentTickDuration);
        } while (!remaining.isNegative() && !remaining.isZero());
        return current;
    }

    private boolean isAllWheelEmpty() {
        return wheels.stream().allMatch(Wheel::isEmpty);
    }

    void cancelTask(TimerTaskHandle handle) {
        this.cancelledHandles.add(handle);
        scheduledTaskCounter.decrementAndGet();
        cancelledTaskCounter.incrementAndGet();
    }

    /**
     * count of tasks added to this timer
     *
     * @return task count
     */
    public int getAllTaskCount() {
        return allTaskCounter.get();
    }

    /**
     * count of tasks scheduled to execute, including cancelled tasks
     *
     * @return task count
     */
    public int getScheduledTaskCount() {
        return scheduledTaskCounter.get();
    }

    /**
     * count of tasks cancelled
     *
     * @return task count
     */
    public int getCancelledTaskCount() {
        return cancelledTaskCounter.get();
    }

    /**
     * count of tasks executed
     *
     * @return task count
     */
    public int getExecutedTaskCount() {
        return executedTaskCounter.get();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {

            lock.lock();
            logger.debug("###LOCK###");
            try {
                while (workerThreadStatus == WorkerThreadStatus.START) {
                    processCancelledTasks();
                    if (firstBucket == null) {
                        // nothing to run, wait
                        logger.debug("no bucket, await");
                        condition.await();
                        logger.debug("no bucket await finished");
                    } else {
                        logger.debug("firstBucket deadline: {}", Formatter.formatInstant(firstBucket.getDeadline()));
                        var delay = Duration.between(Instant.now(), firstBucket.getDeadline());
                        if (!delay.isNegative()) {
                            // delay > 0, wait
                            logger.debug("wait for first bucket, delay: {}", Formatter.formatDuration(delay));
                            var res = condition.awaitNanos(delay.toNanos());
                            if (res > 0) {
                                logger.debug("insufficient delay");
                            }
                        } else {
                            // delay <= 0, time to run
                            logger.debug("run first bucket");
                            var prev = firstBucket.getWheel().getPrev();
                            if (prev != null) {
                                // wheel level is not 1, move task to prev wheel
                                logger.debug("task down wheel");
                                if (!firstBucket.isEmpty()) {
                                    // firstBucket must not be empty for there are tasks to run in it
//                                    Bucket bucket;
//                                    Bucket newFirstBucket = null;

                                    firstBucket.getHandles().stream().map(prev::addTask).min(Comparator.comparing(Bucket::getDeadline)).ifPresent(it -> {
                                        firstBucket.clear();
                                        firstBucket = it;
                                    });

//                                    for (TimerTaskHandle timerTaskHandle : firstBucket.getHandles()) {
//                                        bucket = prev.addTask(timerTaskHandle);
//                                        logger.debug("new bucket deadline: {}", Formatter.formatInstant(bucket.getDeadline()));
//                                        if (newFirstBucket == null) {
//                                            // assign first bucket to newFirstBucket
//                                            newFirstBucket = bucket;
//                                        } else if (bucket.getDeadline().isBefore(newFirstBucket.getDeadline())) {
//                                            newFirstBucket = bucket;
//                                        }
//                                    }
//                                    firstBucket.clear();
//                                    firstBucket = newFirstBucket;
                                    logger.debug("new first bucket deadline: {}", Formatter.formatInstant(firstBucket.getDeadline()));
                                } else {
                                    throw new TimerException("[bug] first bucket is empty");
                                }
                            } else {
                                logger.debug("run task");
                                var executed = firstBucket.runAndClearTasks(executor);
                                scheduledTaskCounter.addAndGet(-executed);
                                executedTaskCounter.addAndGet(executed);
                                firstBucket = findNextBucket().orElse(null);
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.debug("thread interrupted");
            } finally {
                lock.unlock();
                logger.debug("###UNLOCK###");
            }
        }

        private void processCancelledTasks() {
            logger.debug("process cancelled tasks");
            cancelledHandles.forEach(handle -> handle.getBucket().remove(handle));
            cancelledHandles.clear();
        }

        private Optional<Bucket> findNextBucket() {
            return wheels.stream()
                    .map(Wheel::findFirstBucket)
                    .flatMap(Optional::stream)
                    .findFirst();
        }
    }
}
