package io.rapidw.wheeltimer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors

class TimerSpec extends Specification {

    private Logger logger = LoggerFactory.getLogger(TimerSpec.class)

    def "basic"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, ChronoUnit.SECONDS)
        def now = Instant.now()
        timer.start(now)
        logger.debug("adding 5")
        timer.addTask((handle) -> logger.info("run 5"), now.plus(5, ChronoUnit.SECONDS))
        logger.debug("adding 2")
        timer.addTask((handle) -> logger.info("run 2"), now.plus(2, ChronoUnit.SECONDS))
        logger.debug("adding 18")
        timer.addTask((handle) -> logger.info("run 18"), now.plus(18, ChronoUnit.SECONDS))
        logger.debug("sleeping")
        Thread.sleep(20000)
        logger.debug("sleep finished")
    }

    def "basic2"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, ChronoUnit.SECONDS)
        def now = Instant.now()
        timer.start(now)
        logger.debug("adding 2")
        timer.addTask((handle) -> logger.info("run 2"), now.plus(2, ChronoUnit.SECONDS))
        logger.debug("sleep 3")
        Thread.sleep(3000)
        logger.debug("adding 5")
        timer.addTask((handle) -> logger.info("run 5"), now.plus(5, ChronoUnit.SECONDS))
        logger.debug("sleep 8")
        Thread.sleep(8000)
        logger.debug("sleep finished")
    }

    def "basic3"() {
        given:

        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, ChronoUnit.SECONDS)
        logger.debug("adding 2")
        def start = Instant.now()
        Instant end = start
        timer.addTask((handle) -> end = Instant.now(), Duration.of(2, ChronoUnit.SECONDS))
        logger.debug("sleep 10")
        Thread.sleep(10000)

        expect:
        Duration.between(start, end).toMillis() > 1000 && Duration.between(start, end).toMillis() <= 2000
//        logger.debug("adding 5")
//        timer.addTask((handle) -> logger.info("run 5"), Duration.of(5, ChronoUnit.SECONDS))
//        logger.debug("sleep 8")
//        Thread.sleep(8000)
//        logger.debug("sleep finished")
    }

    def "cancel"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, ChronoUnit.SECONDS)
        def now = Instant.now()
        logger.debug("adding 2")
        timer.start(now)
        def handle = timer.addTask((handle) -> logger.info("run 2"), now.plus(2, ChronoUnit.SECONDS))
        Thread.sleep(1000)
        handle.cancel()
        Thread.sleep(2000)
    }

    def "timer stop"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, ChronoUnit.SECONDS)
        def now = Instant.now()
        timer.start(now)
        timer.addTask((handle) -> logger.info("run 3"), now.plus(2, ChronoUnit.SECONDS))
        Thread.sleep(1000)
        def tasks = timer.stop()

        expect:
        tasks.size() == 1
    }
}