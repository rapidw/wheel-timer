package io.rapidw.wheeltimer;

import io.rapidw.wheeltimer.utils.AtomicEnum;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Timer {

    private final LinkedList<Wheel> wheels;
    private final int tickPerWheel;
    private final int tickDuration;
    private final TimeUnit tickTimeUnit;
    private final Thread workerThread;

    private enum WorkerThreadStatus {
        INIT,
        START,
        SHUTDOWN,
    }

    private final AtomicEnum<WorkerThreadStatus> workerThreadStatus;
    // global start time
    private volatile long startTime;
    private final CountDownLatch startTimeInitialized;

    private Queue<TimerTaskHandle> cancelledHandles = new ConcurrentLinkedQueue<>();
    private final MyDelayQueue<Bucket> bucketDelayQueue;


    @Builder
    public Timer(ThreadFactory workerThreadFactory, int tickPerWheel, int tickDuration, TimeUnit tickTimeUnit) {
        this.workerThreadStatus = new AtomicEnum<>(WorkerThreadStatus.INIT);
        this.startTimeInitialized = new CountDownLatch(1);
        this.workerThread = workerThreadFactory.newThread(new Worker());
        this.tickPerWheel = tickPerWheel;
        this.tickDuration = tickDuration;
        this.tickTimeUnit = tickTimeUnit;

        this.wheels = new LinkedList<>();
        this.bucketDelayQueue = new MyDelayQueue<>();
    }

    public void start() {
        switch (workerThreadStatus.get()) {
            case INIT -> {
                if (workerThreadStatus.compareAndSet(WorkerThreadStatus.INIT, WorkerThreadStatus.START)) {
                    workerThread.start();
                }
            }
            case START -> {}
            case SHUTDOWN ->
                throw new TimerException("cannot be started once stopped");
            default ->
                throw new TimerException("invalid worker thread state");
        }

        // wait until startTime is initialized by the worker thread
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignored) {

            }
        }
    }

    public void stop() {
        workerThreadStatus.set(WorkerThreadStatus.SHUTDOWN);
    }

    public TimerTaskHandle addTask(TimerTask task, long delay, TimeUnit timeUnit) {
        // timer must be started before add task
        start();
        // time resolution is millisecond
        val deadline = System.currentTimeMillis() + timeUnit.toMillis(delay);

        val handle = TimerTaskHandle.builder()
                .task(task)
                .deadline(deadline)
                .build();
        val currentMillis = System.currentTimeMillis();

        // 不采用netty的下个tick时处理的方案，因为时间推进是由delayQueue进行的，避免空推进
        synchronized (wheels) {
            // 最大的轮的时长

            Wheel wheel;
            if (wheels.isEmpty()) {
                // 当前没有轮，则新增
                wheel = appendWheel(delay, timeUnit, true);
                val bucket = wheel.addTask(handle);
                bucketDelayQueue.offer(bucket);
                return handle;
            } else {
                // 当前有轮
                wheel = wheels.getLast();
                if (currentMillis + timeUnit.toMillis(delay) - wheel.getBaseTime() > wheel.getDurationMillis()) {
                    // 比当前最大的轮还要长，需要在最后加轮，并放进新加的轮里

                    wheel = appendWheel(delay, timeUnit, false);
                    val bucket = wheel.addTask(handle);
                    bucketDelayQueue.offer(bucket);
                } else {
                    val iter = wheels.descendingIterator();
                    // 找合适的wheel来添加，反向遍历
                    Wheel prev = null;
                    while (iter.hasNext()) {
                        wheel = iter.next();
                        if (currentMillis + timeUnit.toMillis(delay) < wheel.getBaseTime() + wheel.getDurationMillis()) {
                            // 如果延迟在这个轮的范围内
                            prev = wheel;
                        } else if (prev != null) {
                            // 延迟在前一个轮的范围里，但不在后一个轮的范围里，就放进前一个轮里
                            val bucket = wheel.addTask(handle);
                            // 将bucket加入delayQueue
                            bucketDelayQueue.offer(bucket);
                        }
                    }
                    // 循环到最后，放进最小的轮里
                    val bucket = wheel.addTask(handle);
                    bucketDelayQueue.offer(bucket);
                }
            }
            return handle;
        }
    }

    /**
     * 至少增加一个wheel
     * @param delay
     * @param timeUnit
     * @return
     */
    private Wheel appendWheel(long delay, TimeUnit timeUnit, boolean firstWheel) {
        long res = timeUnit.toMillis(delay) / (tickTimeUnit.toMillis(tickDuration) * tickPerWheel);
        int currentTickDuration = tickDuration;
        Wheel prev = null;
        if (!wheels.isEmpty()) {
            prev = wheels.getLast();
        }
        Wheel current;
        current = Wheel.builder()
                .tickCount(tickPerWheel)
                .tickDuration(currentTickDuration)
                .timeUnit(tickTimeUnit)
                .baseTime(this.startTime) // is this correct?
                .prev(prev)
                .build();
        prev = current;
        wheels.addLast(prev);

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

    private static Wheel appendWheel(Wheel prev, int tickPerWheel, int tickDuration, TimeUnit tickTimeUnit) {
        return Wheel.builder()
                .tickCount(tickPerWheel)
                .tickDuration(tickDuration)
                .timeUnit(tickTimeUnit)
                .baseTime(prev.getBaseTime() + prev.getTimeUnit().toMillis(prev.getTickDuration()) * prev.getTickCount())
                .prev(prev)
                .build();
    }

    private class Worker implements Runnable {
        @Override
        @SneakyThrows
        public void run() {
            startTime = System.currentTimeMillis();
            startTimeInitialized.countDown();
            do {
                Bucket bucket = bucketDelayQueue.take();
                log.debug("after take");
                if (bucket.getWheel() == wheels.getFirst()) {
                    log.debug("first wheel bucket, run");
                    bucket.runAndClearTasks();
                } else {
                    // 任务降轮，重新设置baseTime
                    log.debug("NOT first wheel bucket, downgrade");
                    val wheelPrev = bucket.getWheel().getPrev();
                    wheelPrev.setBaseTime(wheelPrev.getBaseTime() + wheelPrev.getDurationMillis());
                    val iter = bucket.iterator();
                    while (iter.hasNext()) {
                        val newBucket =  wheelPrev.addTask(iter.next());
                        bucketDelayQueue.add(newBucket);
                        iter.remove();
                    }
                }
            } while (workerThreadStatus.get() == WorkerThreadStatus.START);
        }
    }
}
