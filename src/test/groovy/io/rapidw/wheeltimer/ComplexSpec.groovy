package io.rapidw.wheeltimer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.RepeatUntilFailure
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ComplexSpec extends Specification {
    private static Logger logger = LoggerFactory.getLogger(ComplexSpec.class)

    @Shared
    private TimeUnit tickUnit
    @Shared
    private int tickPerWheel
    @Shared
    private int tickDuration
    @Shared
    private Timer timer

    def setupSpec() {
        tickUnit = TimeUnit.MILLISECONDS
        tickPerWheel = 10
        tickDuration = 30
        timer = new Timer(Executors.newSingleThreadExecutor(), tickPerWheel, tickDuration, tickUnit)
    }

    @RepeatUntilFailure(maxAttempts = 100)
    def "case1"() {
        given:
        def start = System.nanoTime()
        def end = start
        def delay = 320
        timer.addTask((handle) -> {
            end = System.nanoTime();
            logger.debug("end nanos {}", end)
        }, delay, tickUnit)
        tickUnit.sleep(delay * 2)

        expect:
        end > (start + tickUnit.toNanos(delay) - tickUnit.toNanos(tickDuration)) && end < (start + tickUnit.toNanos(delay) + tickUnit.toNanos(tickDuration))
//        Utils.isBetween(start.plus(delay, tickUnit), end, tickUnit)
    }
}