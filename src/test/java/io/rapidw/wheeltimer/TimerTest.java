package io.rapidw.wheeltimer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TimerTest {

    @Test
    @SneakyThrows
    public void testWheelTimer() {
        val timer = Timer.builder()
                .workerThreadFactory(Executors.defaultThreadFactory())
                .tickDuration(1)
                .tickTimeUnit(TimeUnit.SECONDS)
                .tickPerWheel(3)
                .build();


        timer.addTask((isExpired, isCancelled) -> log.info("run 5"), 5, TimeUnit.SECONDS);
        log.debug("adding 2");
        timer.addTask((isExpired, isCancelled) -> log.info("run 2"), 2, TimeUnit.SECONDS);
        log.debug("sleeping");
        Thread.sleep(8000);
        log.debug("sleep finished");
    }
}
