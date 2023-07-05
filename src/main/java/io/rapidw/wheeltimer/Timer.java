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
     */
    public List<TimerTaskHandle> stop() {
        logger.debug("stopping timer");
        workerThreadStatus = WorkerThreadStatus.SHUTDOWN;
        workerThread.interrupt();
        try {
            workerThread.join();
        } catch (InterruptedException e) {
            logger.error("[bug] interrupted when waiting for worker thread to stop", e);
        }
       	return wheels.stream().flatMap(wheel -> wheel.getAllTasks().stream()).toList();
    }

    public TimerTaskHandle addTask(TimerTask task, Duration delay) {
        return addTask(task, Instant.now().plus(delay));
    }

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
                        var max = baseTime.plus(wheel.getTotalDuration(), wheel.getTimeUnit());
                        return deadline.isBefore(max) && deadline.isAfter(baseTime);
                    }).findFirst();
                    targetWheel.ifPresentOrElse(wheel -> {
                        logger.debug("found wheel, level {}", wheel.getLevel());
                        this.firstBucket = wheel.addTask(handle);
                        condition.signal();
                    }, () -> {
                        logger.debug("not found wheel, build new wheel");
                        var lastWheel = wheels.getLast();
                        var lastWheelBaseTime = lastWheel.findNewBaseTime(now);
                        var newWheel = buildWheel(lastWheelBaseTime, now, deadline);
                        this.firstBucket = newWheel.addTask(handle);
                        condition.signal();
                    });
                } else {
                    var lastWheel = wheels.getLast();
                    var lastWheelBaseTime = lastWheel.findNewBaseTime(now);
                    var lastWheelMax = lastWheelBaseTime.plus(lastWheel.getTotalDuration(), lastWheel.getTimeUnit());
                    if (deadline.isAfter(lastWheelMax)) {
                        // target deadline is after last wheel, need to build a new wheel
                        logger.debug("need new wheel, build it");
                        var newWheel = buildWheel(lastWheelBaseTime, now, deadline);
                        // bucket of new task is always after firstBucket
                        newWheel.addTask(handle);
                        condition.signal();
                    } else {
                        logger.debug("no need new wheel, add to existing wheel");
                        var iter = wheels.descendingIterator();
                        while (iter.hasNext()) {
                            Wheel wheel = iter.next();
                            lastWheelBaseTime = wheel.findNewBaseTime(now);
                            var currentWheelMax = lastWheelBaseTime.plus(wheel.getTotalDuration(), wheel.getTimeUnit());
                            logger.debug("current wheel {}", wheel);
                            if (deadline.isAfter(lastWheelBaseTime) && deadline.isBefore(currentWheelMax)) {
                                // 如果延迟在这个轮的范围内
                                logger.debug("wheel found, add to it");
//                                wheel.setBaseTime(baseTime);
                                var bucket = wheel.addTask(handle);
                                if (bucket.getDeadline().isBefore(firstBucket.getDeadline()))
                                    // 新加入的bucket的deadline比当前最新的deadline更靠前，唤醒worker线程去等待新加入的bucket
                                    this.firstBucket = bucket;
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

    // build wheel should base on new base time
    private Wheel buildWheel(Instant lastWheelBaseTime, Instant now, Instant deadline) {
        logger.debug("building new wheels");
        // 剩余的时长
        Duration remaining;
        if (lastWheelBaseTime != null) {
            remaining = Duration.between(lastWheelBaseTime, deadline);
        } else {
            var baseTime = this.startTime.plus(Duration.between(this.startTime, now).get(this.tickTimeUnit) / tickDuration / this.tickPerWheel * this.tickPerWheel * tickDuration, this.tickTimeUnit);
            remaining = Duration.between(baseTime, deadline);
        }

        Wheel prev = null;
        int currentTickDuration;
        if (!wheels.isEmpty()) {
            prev = wheels.getLast();
            currentTickDuration = prev.getTickDuration() * this.tickPerWheel;
        } else {
            currentTickDuration = this.tickDuration;
            remaining = Duration.between(startTime, deadline);
        }
        logger.debug("remaining {}, currentTickDuration {}", Formatter.formatDuration(remaining), currentTickDuration);
        Wheel current;
        do {
            current = new Wheel(this.tickPerWheel, currentTickDuration, this.tickTimeUnit, this.startTime, prev);
            prev = current;
            wheels.addLast(prev);
            remaining = remaining.minus(current.getTotalDuration(), current.getTimeUnit());
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

    private class Worker implements Runnable {
        @Override
        public void run() {

            lock.lock();
            logger.debug("###LOCK###");
            try {
                while (workerThreadStatus == WorkerThreadStatus.START) {
                    processCancelledTasks();
                    if (firstBucket == null) {
                        // 当前没有定时器
                        logger.debug("no bucket, await");
                        condition.await();
                        logger.debug("no bucket await finished");
                    } else {
                        // 有定时器，取出第一个bucket的延时
                        logger.debug("firstBucket deadline: {}", Formatter.formatInstant(firstBucket.getDeadline()));
                        var delay = Duration.between(Instant.now(), firstBucket.getDeadline());
                        if (!delay.isNegative()) {
                            // 如果延时>0，等待
                            logger.debug("wait for first bucket, delay: {}", Formatter.formatDuration(delay));
                            var res = condition.awaitNanos(delay.toNanos());
                            if (res > 0) {
                                logger.debug("insufficient delay");
                            }
                        } else {
                            // delay <= 0, time to run
                            logger.debug("run first bucket");
                            if (firstBucket.getWheel().getPrev() != null) {
                                // 不是最小的轮，任务降轮
                                logger.debug("task down wheel");
                                var prev = firstBucket.getWheel().getPrev();
                                Bucket bucket;
                                Bucket newFirstBucket = null;
                                if (!firstBucket.isEmpty()) {
                                    for (TimerTaskHandle timerTaskHandle : firstBucket.getHandles()) {
                                        bucket = prev.addTask(timerTaskHandle);
                                        logger.debug("new bucket deadline: {}", Formatter.formatInstant(bucket.getDeadline()));
                                        if (newFirstBucket == null) {
                                            // assign first bucket to newFirstBucket
                                            newFirstBucket = bucket;
                                        } else if (bucket.getDeadline().isBefore(newFirstBucket.getDeadline())) {
                                            newFirstBucket = bucket;
                                        }
                                    }
                                    firstBucket.clear();
                                    firstBucket = newFirstBucket;
                                    logger.debug("new first bucket deadline: {}", Formatter.formatInstant(firstBucket.getDeadline()));
                                } else {
                                    logger.error("[bug] first bucket is empty");
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
            for (TimerTaskHandle handle : cancelledHandles) {
                handle.getBucket().remove(handle);
            }
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
