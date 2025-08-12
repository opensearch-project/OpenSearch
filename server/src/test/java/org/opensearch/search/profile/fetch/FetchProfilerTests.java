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
package org.opensearch.search.profile.fetch;

import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.Timer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.search.profile.Timer.TIMING_TYPE_COUNT_SUFFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class FetchProfilerTests extends OpenSearchTestCase {

    public void testBasicProfiling() {
        FetchProfiler profiler = new FetchProfiler();
        FetchProfileBreakdown root = profiler.startFetchPhase("fetch");
        for (FetchTimingType type : FetchTimingType.values()) {
            if (type == FetchTimingType.PROCESS || type == FetchTimingType.SET_NEXT_READER) {
                continue;
            }
            Timer t = root.getTimer(type);
            t.start();
            t.stop();
        }

        FetchProfileBreakdown child = profiler.startSubPhase("phase", "fetch");
        Timer ct = child.getTimer(FetchTimingType.PROCESS);
        ct.start();
        ct.stop();
        profiler.endFetchPhase("fetch");

        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        ProfileResult profileResult = results.get(0);
        assertEquals("fetch", profileResult.getQueryName());
        Map<String, Long> map = profileResult.getTimeBreakdown();
        assertEquals(10, map.size());
        long sum = 0;
        for (FetchTimingType type : FetchTimingType.values()) {
            if (type == FetchTimingType.PROCESS || type == FetchTimingType.SET_NEXT_READER) {
                continue;
            }
            String key = type.toString();
            assertThat(map.get(key), greaterThan(0L));
            assertThat(map.get(key + TIMING_TYPE_COUNT_SUFFIX), equalTo(1L));
            sum += map.get(key);
        }
        long childTime = ct.getApproximateTiming();
        assertEquals(sum + childTime, profileResult.getTime());
        assertFalse(map.containsKey(FetchTimingType.PROCESS.toString()));
        assertFalse(map.containsKey(FetchTimingType.SET_NEXT_READER.toString()));
    }

    public void testTimerAggregation() {
        FetchProfileBreakdown breakdown = new FetchProfileBreakdown();
        Timer timer = breakdown.getTimer(FetchTimingType.PROCESS);
        timer.start();
        timer.stop();
        timer.start();
        timer.stop();
        Map<String, Long> map = breakdown.toBreakdownMap();
        assertThat(map.get(FetchTimingType.PROCESS.toString()), greaterThan(0L));
        assertThat(map.get(FetchTimingType.PROCESS + TIMING_TYPE_COUNT_SUFFIX), equalTo(2L));
        assertThat(breakdown.toNodeTime(), equalTo(map.get(FetchTimingType.PROCESS.toString())));
    }

    public void testSubPhaseConsolidation() {
        FetchProfiler profiler = new FetchProfiler();

        profiler.startFetchPhase("fetch");
        FetchProfileBreakdown child1 = profiler.startSubPhase("phase", "fetch");
        Timer timer1 = child1.getTimer(FetchTimingType.PROCESS);
        timer1.start();
        timer1.stop();
        profiler.endFetchPhase("fetch");

        profiler.startFetchPhase("fetch");
        FetchProfileBreakdown child2 = profiler.startSubPhase("phase", "fetch");
        Timer timer2 = child2.getTimer(FetchTimingType.PROCESS);
        timer2.start();
        timer2.stop();
        profiler.endFetchPhase("fetch");

        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        ProfileResult profileResult = results.get(0);
        assertEquals("fetch", profileResult.getQueryName());
        assertEquals(1, profileResult.getProfiledChildren().size());
        ProfileResult sub = profileResult.getProfiledChildren().get(0);
        assertEquals("phase", sub.getQueryName());
        Map<String, Long> breakdown = sub.getTimeBreakdown();
        assertThat(breakdown.get(FetchTimingType.PROCESS.toString() + TIMING_TYPE_COUNT_SUFFIX), equalTo(2L));
    }
}
