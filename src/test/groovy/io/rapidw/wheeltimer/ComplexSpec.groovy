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
        tickDuration = 100
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