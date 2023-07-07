package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Timer {
    private static final Logger logger = LoggerFactory.getLogger(Timer.class);

    private final int tickPerWheel;
    private final long tickDuration;
//    private final ChronoUnit tickTimeUnit;

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

    private long startTime;
    private final AtomicInteger allTaskCounter = new AtomicInteger(0);
    private final AtomicInteger scheduledTaskCounter = new AtomicInteger(0);
    private final AtomicInteger cancelledTaskCounter = new AtomicInteger(0);
    private final AtomicInteger executedTaskCounter = new AtomicInteger(0);

    public Timer(Executor executor, int tickPerWheel, int tickDuration, TimeUnit tickTimeUnit) {
        this.workerThreadStatus = WorkerThreadStatus.INIT;
        this.executor = executor;
        this.workerThread = new Thread(new Worker(), "timer-worker");
        this.tickPerWheel = tickPerWheel;
        this.tickDuration = tickTimeUnit.toNanos(tickDuration);
    }

    /**
     * start timer. not mandatory, timer will start when first task is added. only useful when you want to align timer start time
     *
     * @param startTimeNanos start time of timer
     */
    public void start(long startTimeNanos) {
        ensureStarted(startTimeNanos);
    }

    private void ensureStarted(Long startTime) {
        logger.debug("starting timer");
        switch (workerThreadStatus) {
            case INIT -> {
                workerThreadStatus = WorkerThreadStatus.START;
                this.startTime = Objects.requireNonNullElseGet(startTime, System::nanoTime);
                workerThread.start();
            }
            case START -> {
            }
            case SHUTDOWN -> throw new TimerException("cannot be started once stopped");
            default -> throw new TimerException("[bug] invalid worker thread state");
        }
    }

    /**
     * shutdown timer and return all uncanceled tasks
     *
     * @return all uncanceled tasks
     */
    public List<TimerTaskHandle> stop() {
        logger.debug("stopping timer");
        lock.lock();
        workerThreadStatus = WorkerThreadStatus.SHUTDOWN;
        lock.unlock();
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
    public TimerTaskHandle addTask(TimerTask task, int delay, TimeUnit delayUnit) {
        return addTask(task, System.nanoTime() + delayUnit.toNanos(delay));
    }

    /**
     * add task to timer, deadline is the instant to run task. Task will be executed one tickDuration early at most.
     *
     * @param task     task to run
     * @param deadline deadline to run task
     * @return handle to cancel task
     */
    public TimerTaskHandle addTask(TimerTask task, long deadline) {
        logger.debug("add task, deadline: {}", Formatter.formatInstant(deadline));
        // timer must be started before add task
        ensureStarted(null);

        var now = System.nanoTime();
        var handle = new TimerTaskHandle(this, task, deadline);
        if (deadline < now) {
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
                var newWheel = buildWheel(null,null, now, deadline);
                this.firstBucket = newWheel.addTask(handle);
                condition.signal();
            } else {
                logger.debug("has wheels");
                // has wheels, find the right wheel to add task
                if (isAllWheelEmpty()) {
                    logger.debug("all wheel empty, wheel list size {}", wheels.size());
                    AtomicInteger i = new AtomicInteger(0);
                    var targetWheel = wheels.stream().filter(wheel -> {

                        var baseTime = wheel.findNewBaseTime(now);
                        logger.debug("current wheel level {}, new baseTime {}, i {}", wheel.getLevel(), Formatter.formatInstant(baseTime), i.getAndIncrement());
                        var maxTime = baseTime + wheel.getTotalDuration();
                        return deadline < maxTime && deadline > baseTime;
                    }).findFirst();

                    targetWheel.ifPresentOrElse(wheel -> {
                        logger.debug("found wheel, level {}", wheel.getLevel());
                        this.firstBucket = wheel.addTask(handle);
                        condition.signal();
                    }, () -> {
                        logger.debug("not found wheel, build new wheel");
                        var wheel = wheels.getLast();
                        var baseTime = wheel.findNewBaseTime(now);
                        this.firstBucket = buildWheel(wheel, baseTime, now, deadline).addTask(handle);
                        condition.signal();
                    });
                } else {
                    var wheel = wheels.getLast();
                    var baseTime = wheel.findNewBaseTime(now);
                    var maxTime = baseTime + wheel.getTotalDuration();
                    if (deadline > maxTime) {
                        // target deadline is after last wheel, need to build a new wheel
                        logger.debug("need new wheel, build it");
                        buildWheel(wheel, baseTime, now, deadline).addTask(handle);
                        // bucket of new task is always after firstBucket, no need to update firstBucket
                        condition.signal();
                    } else {
                        logger.debug("no need new wheel, add to existing wheel");
                        var iter = wheels.descendingIterator();
                        while (iter.hasNext()) {
                            wheel = iter.next();
                            baseTime = wheel.findNewBaseTime(now);
                            maxTime = baseTime + wheel.getTotalDuration();
                            logger.debug("current wheel {}", wheel);
                            if (deadline > baseTime && deadline < maxTime) {
                                // deadline in this wheel
                                logger.debug("wheel found, add to it");
                                var bucket = wheel.addTask(handle);
                                if (bucket.getDeadline() < firstBucket.getDeadline()) {
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
     * @param prevBaseTime base time of last wheel
     * @param now               current time
     * @param deadline          deadline of new task
     * @return last wheel of new wheels
     */
    private Wheel buildWheel(Wheel prev, Long prevBaseTime, long now, long deadline) {
        logger.debug("building new wheels");
        long remaining;
        Wheel current;
        long currentTickDuration;
        if (prevBaseTime != null) {
            // has wheels
            logger.debug("has wheel");
            remaining = deadline - prevBaseTime;
            currentTickDuration = wheels.getLast().getTickDuration() * this.tickPerWheel;
        } else {
            // compute baseTime based on startTime and now
            var baseTime = Wheel.computeTime(this.startTime, now, this.tickPerWheel, this.tickDuration);
            logger.debug("no wheel, new baseTime {}, now {}", Formatter.formatInstant(baseTime), Formatter.formatInstant(now));
            remaining = deadline - baseTime;
            currentTickDuration = this.tickDuration;
        }

        logger.debug("remaining {}, currentTickDuration {}", Formatter.formatDuration(remaining), currentTickDuration);
        do {
            // baseTime will be updated in addTask
            current = new Wheel(this.tickPerWheel, currentTickDuration, this.startTime, prev);
            prev = current;
            wheels.addLast(prev);
            remaining = remaining - current.getTotalDuration();
            currentTickDuration = currentTickDuration * this.tickPerWheel;
            logger.debug("new remaining {}, new currentTickDuration {}", Formatter.formatDuration(remaining), currentTickDuration);
        } while (remaining > 0);
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
                    logger.debug("new current nanos {}", System.nanoTime());
                    processCancelledTasks();
                    if (firstBucket == null) {
                        // nothing to run, wait
//                        logger.debug("no bucket, await");
                        condition.await();
//                        logger.debug("no bucket await finished");
                    } else {
//                        logger.debug("firstBucket deadline: {}, current nanos {}", Formatter.formatInstant(firstBucket.getDeadline()), System.nanoTime());
                        var delay = firstBucket.getDeadline() - System.nanoTime();
                        if (delay > 0) {
                            // delay > 0, wait
                            logger.debug("wait for first bucket, delay: {}", Formatter.formatDuration(delay));
                            var res = condition.awaitNanos(delay);
                            if (res > 0) {
                                logger.debug("insufficient delay");
                            }
                        } else {
                            // delay <= 0, time to run
//                            logger.debug("run first bucket");
                            var prev = firstBucket.getWheel().getPrev();
                            if (prev != null) {
                                // wheel level is not 1, move task to prev wheel
//                                logger.debug("task down wheel");
                                if (!firstBucket.isEmpty()) {
                                    // firstBucket must not be empty for there are tasks to run in it
                                    firstBucket.getHandles().stream().map(prev::addTask).min(Comparator.comparing(Bucket::getDeadline)).ifPresent(it -> {
                                        firstBucket.clear();
                                        firstBucket = it;
                                    });
//                                    logger.debug("new first bucket deadline: {}", Formatter.formatInstant(firstBucket.getDeadline()));
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
