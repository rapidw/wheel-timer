package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.AtomicEnum;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
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
    // global start time
    private Instant startTime;
    private final CountDownLatch startTimeInitialized;

    private Queue<TimerTaskHandle> cancelledHandles = new ConcurrentLinkedQueue<>();
//    private final MyDelayQueue<Bucket> bucketDelayQueue;

    @Builder
    public Timer(ThreadFactory workerThreadFactory, int tickPerWheel, int tickDuration, ChronoUnit tickTimeUnit) {
        this.workerThreadStatus = new AtomicEnum<>(WorkerThreadStatus.INIT);
        this.startTimeInitialized = new CountDownLatch(1);
        this.workerThread = workerThreadFactory.newThread(new Worker());
        this.tickPerWheel = tickPerWheel;
        this.tickDuration = tickDuration;
        this.tickTimeUnit = tickTimeUnit;
        this.startTime = Instant.now();
    }

    public void start() {
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

        // wait until startTime is initialized by the worker thread
//        while (startTime == 0) {
//            try {
//                startTimeInitialized.await();
//            } catch (InterruptedException ignored) {
//
//            }
//        }
    }

    public void stop() {
        workerThreadStatus.set(WorkerThreadStatus.SHUTDOWN);
    }

    public TimerTaskHandle addTask(TimerTask task, Instant deadline, TimeUnit timeUnit) {
        // timer must be started before add task
        start();

        val handle = TimerTaskHandle.builder()
                .task(task)
                .deadline(deadline)
                .build();
        val current = Instant.now();
//        val currentMillis = System.currentTimeMillis();

        // 不采用netty的下个tick时处理的方案，因为时间推进是由delayQueue进行的，避免空推进
        lock.lock();
        // 最大的轮的时长

//        Wheel wheel;
        if (wheels.isEmpty()) {
            // 当前没有轮，则新增
            val newWheel = appendWheel(deadline, timeUnit, true);
            firstBucket = newWheel.addTask(handle);
            condition.signal();
            return handle;
        } else {
            // 当前有轮
            val lastWheel = wheels.getLast();
            if (!Duration.between(lastWheel.getBaseTime(), deadline).minus(lastWheel.getTotalDuration(), lastWheel.getTimeUnit()).isNegative()) {
                // 比当前最大的轮还要长，需要在最后加轮，并放进新加的轮里

//                wheel = appendWheel(delay, timeUnit, false);
                val newWheel = buildWheel(deadline);
                val bucket = newWheel.addTask(handle);
                if (Duration.between(bucket.getDeadline(), firstBucket.getDeadline()).isNegative()) {
                    // 新加入的bucket的deadline比当前最新的deadline更靠前，唤醒worker线程去等待新加入的bucket
                    firstBucket = bucket;
                    condition.signal();
                }
            } else {
                val iter = wheels.descendingIterator();
                // 找合适的wheel来添加，反向遍历
                Wheel prev = null;
                Wheel wheel = null;
                while (iter.hasNext()) {
                    wheel = iter.next();
//                    if (currentMillis + timeUnit.toMillis(delay) < wheel.getBaseTime() + wheel.getDurationMillis()) {
                    if (Duration.between(wheel.getBaseTime(), deadline).minus(wheel.getTotalDuration(), wheel.getTimeUnit()).isNegative()) {
                        // 如果延迟在这个轮的范围内
                        prev = wheel;
                    } else if (prev != null) {
                        // 延迟在前一个轮的范围里，但不在后一个轮的范围里，就放进前一个轮里
                        // 将bucket加入delayQueue
                        firstBucket = wheel.addTask(handle);
                        condition.signal();
                    }
                }
                // 循环到最后，放进最小的轮里
                if (wheel != null) {
                    firstBucket = wheel.addTask(handle);
                    condition.signal();
                }
            }
        }
        return handle;
    }

    private Wheel buildWheel(Instant deadline) {
        // 剩余的时长
        Duration remaining;
        Wheel prev = null;
        int currentTickDuration;
        if (!wheels.isEmpty()) {
            prev = wheels.getLast();
            currentTickDuration = prev.getTickDuration() * this.tickPerWheel;
        } else {
            currentTickDuration = this.tickDuration;
            remaining = Duration.between(startTime, deadline);
        }
        Wheel current;
        current = Wheel.builder()
                .tickCount(this.tickPerWheel)
                .tickDuration(currentTickDuration)
                .timeUnit(this.tickTimeUnit)
                .baseTime(this.startTime) // is this correct?
                .prev(prev)
                .build();
        prev = current;
        wheels.addLast(prev);


    }

    /**
     * 至少增加一个wheel
     *
     * @param delay
     * @param timeUnit
     * @return
     */
    private Wheel appendWheel(long delay, TimeUnit timeUnit, boolean firstWheel) {
        long res = timeUnit.toMillis(delay) / (tickTimeUnit.toMillis(this.tickDuration) * this.tickPerWheel);
        int currentTickDuration = this.tickDuration;
        Wheel prev = null;
        if (!wheels.isEmpty()) {
            prev = wheels.getLast();
        }
        Wheel current;
        current = Wheel.builder()
                .tickCount(this.tickPerWheel)
                .tickDuration(currentTickDuration)
                .timeUnit(this.tickTimeUnit)
                .baseTime(this.startTime) // is this correct?
                .prev(prev)
                .build();
        prev = current;
        wheels.addLast(prev);

        // total duration of next wheel
        currentTickDuration = currentTickDuration * tickPerWheel;
        while (res > 0) {
            current = Wheel.builder()
                    .tickCount(tickPerWheel)
                    .tickDuration(currentTickDuration)
                    .timeUnit(tickTimeUnit)
                    .baseTime(this.startTime) // is this correct?
                    .prev(prev)
                    .build();
            wheels.addLast(current);
            prev = current;

            res = res / ((long) tickDuration * tickPerWheel);
        }
        log.debug("current wheel list size={}", wheels.size());
        return current;
    }

    private class Worker implements Runnable {
        @Override
        @SneakyThrows
        public void run() {
//            startTime = System.currentTimeMillis();
//            startTimeInitialized.countDown();
            do {
                lock.lock();
                while (true) {
                    if (firstBucket == null) {
                        // 当前没有定时器
                        condition.await();
                    } else {
                        // 有定时器，取出第一个bucket的延时
                        val delay = firstBucket.getDeadline(TimeUnit.MILLISECONDS) - System.currentTimeMillis();
                        if (delay > 0) {
                            // 如果延时>0，等待
                            condition.await(delay, TimeUnit.MILLISECONDS);
                        } else {
                            // 延时<0，立即执行
                            firstBucket.runAndClearTasks();
                            // 任务降轮？
                            break;
                        }
                    }
                }
                lock.unlock();
            } while (workerThreadStatus.get() == WorkerThreadStatus.START);
        }
    }
}
