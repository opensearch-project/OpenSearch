/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.profile;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

public class TimerTests extends OpenSearchTestCase {

    public void testTimingInterval() {
        final AtomicLong nanoTimeCallCounter = new AtomicLong();
        Timer t = new Timer() {
            long time = 50;

            @Override
            long nanoTime() {
                nanoTimeCallCounter.incrementAndGet();
                return time += 1;
            }
        };
        for (int i = 0; i < 100000; ++i) {
            t.start();
            t.stop();
            if (i < 256) {
                // for the first 256 calls, nanoTime() is called
                // once for `start` and once for `stop`
                assertEquals((i + 1) * 2, nanoTimeCallCounter.get());
            }
        }
        // only called nanoTime() 3356 times, which is significantly less than 100000
        assertEquals(3356L, nanoTimeCallCounter.get());
    }

    public void testExtrapolate() {
        Timer t = new Timer() {
            long time = 50;

            @Override
            long nanoTime() {
                return time += 42;
            }
        };
        t.start();
        t.stop();
        long timerStartTime = t.getEarliestTimerStartTime();
        for (int i = 2; i < 100000; ++i) {
            t.start();
            t.stop();
            assertEquals(i, t.getCount());
            assertEquals(timerStartTime, t.getEarliestTimerStartTime());
            // Make sure the cumulated timing is 42 times the number of calls as expected
            assertEquals(i * 42L, t.getApproximateTiming());
        }
    }

}
