package io.rapidw.wheeltimer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors

class TimerSpec extends Specification {

    private Logger logger = LoggerFactory.getLogger(TimerSpec.class)

    def "basic"() {
        given:
        def timer = Timer.builder()
                .workerThreadFactory(Executors.defaultThreadFactory())
                .tickDuration(1)
                .tickTimeUnit(ChronoUnit.SECONDS)
                .tickPerWheel(3)
                .build();
        def now = Instant.now()
        logger.debug("adding 5")
        timer.addTask((handle) -> logger.info("run 5"), now.plus(5, ChronoUnit.SECONDS))
        logger.debug("adding 2")
        timer.addTask((handle) -> logger.info("run 2"), now.plus(2, ChronoUnit.SECONDS))
        logger.debug("sleeping")
        timer.addTask((handle) -> logger.info("run 18"), now.plus(18, ChronoUnit.SECONDS))
        Thread.sleep(20000)
        logger.debug("sleep finished")
    }

    def "cancel"() {
        given:
        def timer = Timer.builder()
                .workerThreadFactory(Executors.defaultThreadFactory())
                .tickDuration(1)
                .tickTimeUnit(ChronoUnit.SECONDS)
                .tickPerWheel(3)
                .build();
        def now = Instant.now()
        logger.debug("adding 2")
        def handle = timer.addTask((handle) -> logger.info("run 2"), now.plus(2, ChronoUnit.SECONDS))
        Thread.sleep(1000)
        handle.cancel()
        Thread.sleep(2000)
    }
}