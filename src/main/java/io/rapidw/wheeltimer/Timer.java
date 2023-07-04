package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.AtomicEnum;
import io.rapidw.wheeltimer.utils.Formatter;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.slf4j.LoggerFactory;
import org.w3c.dom.ls.LSException;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Timer {

    private final int tickPerWheel;
    private final int tickDuration;
    private final ChronoUnit tickTimeUnit;
    private final Thread workerThread;

    private final LinkedList<Wheel> wheels = new LinkedList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile Bucket firstBucket;

    private enum WorkerThreadStatus {
        INIT,
        START,
        SHUTDOWN,
    }

    private final AtomicEnum<WorkerThreadStatus> workerThreadStatus;
    private final Instant startTime;

    private Queue<TimerTaskHandle> cancelledHandles = new ConcurrentLinkedQueue<>();

    @Builder
    public Timer(ThreadFactory workerThreadFactory, int tickPerWheel, int tickDuration, ChronoUnit tickTimeUnit) {
        this.workerThreadStatus = new AtomicEnum<>(WorkerThreadStatus.INIT);
        this.workerThread = workerThreadFactory.newThread(new Worker());
        this.tickPerWheel = tickPerWheel;
        this.tickDuration = tickDuration;
        this.tickTimeUnit = tickTimeUnit;
        this.startTime = Instant.now();
    }

    public void start() {
        log.debug("start timer");
        switch (workerThreadStatus.get()) {
            case INIT -> {
                if (workerThreadStatus.compareAndSet(WorkerThreadStatus.INIT, WorkerThreadStatus.START)) {
                    workerThread.start();
                }
            }
            case START -> {
            }
            case SHUTDOWN -> throw new TimerException("cannot be started once stopped");
            default -> throw new TimerException("invalid worker thread state");
        }
    }

    /**
     * return all uncanceled tasks
     */
    public List<TimerTaskHandle> stop() {
        workerThreadStatus.set(WorkerThreadStatus.SHUTDOWN);
        return null;
    }

    public TimerTaskHandle addTask(TimerTask task, Instant deadline) {
        log.debug("add task, deadline: {}", Formatter.formatInstant(deadline));
        // timer must be started before add task
        start();

        val handle = TimerTaskHandle.builder()
                .timer(this)
                .task(task)
                .deadline(deadline)
                .build();

        // 不采用netty的下个tick时处理的方案，因为时间推进是由delayQueue进行的，避免空推进
        lock.lock();

        if (wheels.isEmpty()) {
            log.debug("no wheel");
            // 当前没有轮，则新增
            val newWheel = buildWheel(null, deadline);
            this.firstBucket = newWheel.addTask(handle);
            condition.signal();
        } else {
            // 当前有轮
            log.debug("has wheels");
            val lastWheel = wheels.getLast();
            val lastWheelMax = lastWheel.getBaseTime().plus(lastWheel.getTotalDuration(), lastWheel.getTimeUnit());
            if (deadline.isAfter(lastWheelMax)) {
                // 比当前最大的轮还要长，需要在最后加轮，并放进新加的轮里
                log.debug("need new wheel, build it");

                val newWheel = buildWheel(lastWheel, deadline);
                val bucket = newWheel.addTask(handle);
                if (bucket.getDeadline().isBefore(firstBucket.getDeadline()))
                    // 新加入的bucket的deadline比当前最新的deadline更靠前，唤醒worker线程去等待新加入的bucket
                    this.firstBucket = bucket;
                condition.signal();
            } else {
                log.debug("no need new wheel, add to existing wheel");
                val iter = wheels.descendingIterator();
                // 找合适的wheel来添加，反向遍历

                while (iter.hasNext()) {
                    Wheel wheel = iter.next();
                    val currentWheelMax = wheel.getBaseTime().plus(wheel.getTotalDuration(), wheel.getTimeUnit());
                    val currentWheelMin = wheel.getBaseTime().plus(wheel.getTickDuration(), wheel.getTimeUnit());
                    log.debug("current wheel {}", wheel);
                    if (deadline.isAfter(currentWheelMin) && deadline.isBefore(currentWheelMax)) {
                        // 如果延迟在这个轮的范围内
                        log.debug("wheel found, add to it");
                        val bucket = wheel.addTask(handle);
                        if (bucket.getDeadline().isBefore(firstBucket.getDeadline()))
                            // 新加入的bucket的deadline比当前最新的deadline更靠前，唤醒worker线程去等待新加入的bucket
                            this.firstBucket = bucket;
                        condition.signal();
                        break;
                    } else {
                        log.debug("not this wheel, continue");
                    }
                }
            }
        }
        lock.unlock();
        return handle;
    }

    private Wheel buildWheel(Wheel lastWheel, Instant deadline) {
        // 剩余的时长
        Duration remaining;
        if (lastWheel != null) {
            remaining = Duration.between(lastWheel.getBaseTime(), deadline);
        } else {
            remaining = Duration.between(startTime, deadline);
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
        log.debug("remaining {}, currentTickDuration {}", Formatter.formatDuration(remaining), currentTickDuration);
        Wheel current;
        do {
            current = Wheel.builder()
                    .tickCount(this.tickPerWheel)
                    .tickDuration(currentTickDuration)
                    .timeUnit(this.tickTimeUnit)
                    .baseTime(this.startTime) // 最后一个轮的basetime作为新轮的basetime
                    .prev(prev)
                    .build();
            prev = current;
            wheels.addLast(prev);
            remaining = remaining.minus(current.getTotalDuration(), current.getTimeUnit());
            currentTickDuration = currentTickDuration * this.tickPerWheel;
            log.debug("new remaining {}, new currentTickDuration {}", Formatter.formatDuration(remaining), currentTickDuration);
        } while (!remaining.isNegative() && !remaining.isZero());

        return current;
    }

    void cancelTask(TimerTaskHandle handle) {
        this.cancelledHandles.add(handle);
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            lock.lock();
            log.debug("###LOCK###");
            try {
                while (workerThreadStatus.get() == WorkerThreadStatus.START) {
                    processCancelledTasks();
                    if (firstBucket == null) {
                        // 当前没有定时器
                        log.debug("no bucket, await");
                        condition.await();
                        log.debug("no bucket await finished");
                    } else {
                        // 有定时器，取出第一个bucket的延时
                        log.debug("firstBucket deadline: {}", Formatter.formatInstant(firstBucket.getDeadline()));
                        val delay = Duration.between(Instant.now(), firstBucket.getDeadline());
                        if (!delay.isNegative()) {
                            // 如果延时>0，等待
                            log.debug("wait for first bucket, delay: {}", delay);
                            val res = condition.awaitNanos(delay.toNanos());
                            if (res > 0) {
                                log.debug("insufficient delay");
                            }
                        } else {
                            // 延时<0，立即执行
                            log.debug("run first bucket");
                            if (firstBucket.getWheel().getPrev() != null) {
                                // 不是最小的轮，任务降轮
                                log.debug("task down wheel");
                                val prev = firstBucket.getWheel().getPrev();
                                Bucket bucket;
                                Bucket newFirstBucket = null;
                                for (TimerTaskHandle timerTaskHandle : firstBucket) {
                                    bucket = prev.addTask(timerTaskHandle);
                                    log.debug("new bucket deadline: {}", Formatter.formatInstant(bucket.getDeadline()));
                                    if (newFirstBucket == null) {
                                        newFirstBucket = bucket;
                                    } else if (bucket.getDeadline().isBefore(newFirstBucket.getDeadline())) {
                                        newFirstBucket = bucket;
                                    }
                                }
                                firstBucket.clear();
                                firstBucket = newFirstBucket;
                                log.debug("new first bucket deadline: {}", Formatter.formatInstant(firstBucket.getDeadline()));
                            } else {
                                log.debug("run task");
                                firstBucket.runAndClearTasks();
                                firstBucket = findNextBucket();
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                log.debug("thread interrupted");
            } finally {
                lock.unlock();
                log.debug("###UNLOCK###");
            }
        }

        private void processCancelledTasks() {
            log.debug("process cancelled tasks");
            for (TimerTaskHandle handle : cancelledHandles) {
                handle.getBucket().remove(handle);
            }
            cancelledHandles.clear();
        }

        private Bucket findNextBucket() {
            for (Wheel wheel : wheels) {
                val bucket = wheel.findFirstBucket();
                if (bucket != null) {
                    return bucket;
                }
            }
            return null;
        }
    }
}
