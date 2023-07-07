package io.rapidw.wheeltimer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import static java.time.temporal.ChronoUnit.SECONDS


class TimerSpec extends Specification {

    private Logger logger = LoggerFactory.getLogger(TimerSpec.class)

    def "basic"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, SECONDS)
        def now = Instant.now()
        timer.start(now)
        logger.debug("adding 5")
        timer.addTask((handle) -> logger.info("run 5"), now.plus(5, SECONDS))
        logger.debug("adding 2")
        timer.addTask((handle) -> logger.info("run 2"), now.plus(2, SECONDS))
        logger.debug("adding 18")
        timer.addTask((handle) -> logger.info("run 18"), now.plus(18, SECONDS))
        logger.debug("sleeping")
        Thread.sleep(20000)
        logger.debug("sleep finished")
    }

    def "basic2"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, SECONDS)
        def now = Instant.now()
        timer.start(now)
        logger.debug("adding 2")
        timer.addTask((handle) -> logger.info("run 2"), now.plus(2, SECONDS))
        logger.debug("sleep 3")
        Thread.sleep(3000)
        logger.debug("adding 5")
        timer.addTask((handle) -> logger.info("run 5"), now.plus(5, SECONDS))
        logger.debug("sleep 8")
        Thread.sleep(8000)
        logger.debug("sleep finished")
    }

    def "basic3"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), tickPerWheel, tickDuration, tickUnit)
        def start = Instant.now()
        Instant end = start
        timer.addTask((handle) -> end = Instant.now(), Duration.of(delay, tickUnit))
        TimeUnit.of(tickUnit).sleep(sleep)

        expect:
        Utils.isBetween(start.plus(delay, tickUnit), end, tickUnit)

        where:
        tickPerWheel | tickDuration | tickUnit | delay | sleep
        3            | 1            | SECONDS  | 2     | 10
    }

    def "cancel"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, SECONDS)
        def now = Instant.now()
        logger.debug("adding 2")
        timer.start(now)
        def handle = timer.addTask((handle) -> logger.info("run 2"), now.plus(2, SECONDS))
        Thread.sleep(1000)
        handle.cancel()
        Thread.sleep(2000)
    }

    def "timer stop"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, SECONDS)
        def now = Instant.now()
        timer.start(now)
        timer.addTask((handle) -> logger.info("run 3"), now.plus(2, SECONDS))
        Thread.sleep(1000)
        def tasks = timer.stop()

        expect:
        tasks.size() == 1
    }


}