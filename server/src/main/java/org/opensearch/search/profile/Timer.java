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

import java.util.HashMap;
import java.util.Map;

/** Helps measure how much time is spent running some methods.
 *  The {@link #start()} and {@link #stop()} methods should typically be called
 *  in a try/finally clause with {@link #start()} being called right before the
 *  try block and {@link #stop()} being called at the beginning of the finally
 *  block:
 *  <pre>
 *  timer.start();
 *  try {
 *    // code to time
 *  } finally {
 *    timer.stop();
 *  }
 *  </pre>
 *
 *  @opensearch.internal
 */
public class Timer extends ProfileMetric {
    public static final String TIMING_TYPE_COUNT_SUFFIX = "_count";
    public static final String TIMING_TYPE_START_TIME_SUFFIX = "_start_time";

    private boolean doTiming;
    private long timing, count, lastCount, start, earliestTimerStartTime;

    public Timer(String name) {
        super(name);
    }

    public Timer(long timing, long count, long lastCount, long start, long earliestTimerStartTime, String name) {
        super(name);
        this.timing = timing;
        this.count = count;
        this.lastCount = lastCount;
        this.start = start;
        this.earliestTimerStartTime = earliestTimerStartTime;
    }

    /** pkg-private for testing */
    long nanoTime() {
        return System.nanoTime();
    }

    /** Start the timer. */
    public final void start() {
        assert start == 0 : "#start call misses a matching #stop call";
        // We measure the timing of each method call for the first 256
        // calls, then 1/2 call up to 512 then 1/3 up to 768, etc. with
        // a maximum interval of 1024, which is reached for 1024*2^8 ~= 262000
        // This allows to not slow down things too much because of calls
        // to System.nanoTime() when methods are called millions of time
        // in tight loops, while still providing useful timings for methods
        // that are only called a couple times per search execution.
        doTiming = (count - lastCount) >= Math.min(lastCount >>> 8, 1024);
        if (doTiming) {
            start = nanoTime();
            if (count == 0) {
                earliestTimerStartTime = start;
            }
        }
        count++;
    }

    /** Stop the timer. */
    public final void stop() {
        if (doTiming) {
            timing += (count - lastCount) * Math.max(nanoTime() - start, 1L);
            lastCount = count;
            start = 0;
        }
    }

    /** Return the number of times that {@link #start()} has been called. */
    public final long getCount() {
        if (start != 0) {
            throw new IllegalStateException("#start call misses a matching #stop call");
        }
        return count;
    }

    /** Return the timer start time in nanoseconds.*/
    public final long getEarliestTimerStartTime() {
        if (start != 0) {
            throw new IllegalStateException("#start call misses a matching #stop call");
        }
        return earliestTimerStartTime;
    }

    /** Return an approximation of the total time spent between consecutive calls of #start and #stop. */
    public final long getApproximateTiming() {
        if (start != 0) {
            throw new IllegalStateException("#start call misses a matching #stop call");
        }
        // We don't have timings for the last `count-lastCount` method calls
        // so we assume that they had the same timing as the lastCount first
        // calls. This approximation is ok since at most 1/256th of method
        // calls have not been timed.
        long timing = this.timing;
        if (count > lastCount) {
            assert lastCount > 0;
            timing += (count - lastCount) * timing / lastCount;
        }
        return timing;
    }

    @Override
    public Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = new HashMap<>();
        map.put(getName(), getApproximateTiming());
        map.put(getName() + TIMING_TYPE_COUNT_SUFFIX, getCount());
        map.put(getName() + TIMING_TYPE_START_TIME_SUFFIX, getEarliestTimerStartTime());
        return map;
    }
}
