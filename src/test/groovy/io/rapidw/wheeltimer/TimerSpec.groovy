/*
 * Copyright 2023 Rapidw
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rapidw.wheeltimer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class TimerSpec extends Specification {

    private Logger logger = LoggerFactory.getLogger(TimerSpec.class)

    def "basic"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, TimeUnit.SECONDS)
        def now = Instant.now()
//        timer.start(System.nanoTime())
        logger.debug("adding 5")
        timer.addTask((handle) -> logger.info("run 5"), 5, TimeUnit.SECONDS)
        logger.debug("adding 2")
        timer.addTask((handle) -> logger.info("run 2"), 2, TimeUnit.SECONDS)
        logger.debug("adding 18")
        timer.addTask((handle) -> logger.info("run 18"), 18, TimeUnit.SECONDS)
        logger.debug("sleeping")
        Thread.sleep(20000)
        logger.debug("sleep finished")
    }

    def "basic2"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, TimeUnit.SECONDS)
        logger.debug("adding 2")
        timer.addTask((handle) -> logger.info("run 2"), 2, TimeUnit.SECONDS)
        logger.debug("sleep 3")
        Thread.sleep(3000)
        logger.debug("adding 5")
        timer.addTask((handle) -> logger.info("run 5"), 5, TimeUnit.SECONDS)
        logger.debug("sleep 8")
        Thread.sleep(8000)
        logger.debug("sleep finished")
    }

    def "basic3"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), tickPerWheel, tickDuration, tickUnit)
        def start = Instant.now()
        Instant end = start
        timer.addTask((handle) -> end = Instant.now(), delay, tickUnit)
        tickUnit.sleep(sleep)

        expect:
        Utils.isBetween(start.plus(delay, tickUnit.toChronoUnit()), end, tickUnit.toChronoUnit())

        where:
        tickPerWheel | tickDuration | tickUnit | delay | sleep
        3            | 1            | TimeUnit.SECONDS  | 2     | 10
    }

    def "cancel"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, TimeUnit.SECONDS)
        logger.debug("adding 2")
        def handle = timer.addTask((handle) -> logger.info("run 2"), 2, TimeUnit.SECONDS)
        Thread.sleep(1000)
        handle.cancel()
        Thread.sleep(2000)
    }

    def "timer stop"() {
        given:
        def timer = new Timer(Executors.newSingleThreadExecutor(), 3, 1, TimeUnit.SECONDS)
        timer.addTask((handle) -> logger.info("run 3"), 3, TimeUnit.SECONDS)
        Thread.sleep(1000)
        def tasks = timer.stop()

        expect:
        tasks.size() == 1
    }


}