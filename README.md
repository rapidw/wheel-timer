# Rapidw Wheel Timer
A Better Wheel Timer implemented by Java and inspired by Kafka `TimerWheel` and Netty `HashedWheelTimer`.
This timer does `NOT` execute scheduled task on time, possibly error is up to one `tickDuration` long. 

## Features
- Task add/cancel
- Require a executor to execute task
- Some statistics

## Parameters
- tickDuration: The length of a tick
- ticksPerWheel: The number of ticks per wheel
- tickTimeUnit: The unit of tickDuration

Note: Due to limitation of Java, tickDuration should be greater than 40ms, otherwise tasks will be executed too late.

## Usage
```java
Instant now = Instant.now();
Timer timer = new Timer(Executors.newSingleThreadExecutor(), tickPerWheel, tickDuration, tickUnit)
timer.start(now);
timer.addTask((handle) -> {
            // your code
        }, 10, TimeUnit.SECONDS)
TimeUnit.SECONDS.sleep(20);
timer.stop();
```
`timer.start()` can be omitted if you don't want to align timer start time to task deadline, it will be called when you add task.

## Statistics
- allTaskCount: The number of all tasks add to this timer
- scheduledTaskCount: The number of tasks that have been scheduled, including cancelled tasks
- cancelledTaskCount: The number of tasks that have been cancelled
- executedTaskCount: The number of tasks that have been executed

## Performance
Instead of min heap, this timer is built on the concept of `Hashed and Hierarchical Timing Wheels`, 
so the time complexity of add/cancel task is `O(1)`, and the space complexity is `O(n)`.