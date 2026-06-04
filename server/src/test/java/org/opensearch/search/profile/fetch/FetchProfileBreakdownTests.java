/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.search.profile.Timer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.opensearch.search.profile.Timer.TIMING_TYPE_COUNT_SUFFIX;
import static org.opensearch.search.profile.Timer.TIMING_TYPE_START_TIME_SUFFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class FetchProfileBreakdownTests extends OpenSearchTestCase {

    public void testBreakdownMap() {
        FetchProfileBreakdown breakdown = new FetchProfileBreakdown();
        for (FetchTimingType type : FetchTimingType.values()) {
            Timer t = breakdown.getTimer(type);
            t.start();
            t.stop();
        }
        Map<String, Long> map = breakdown.toBreakdownMap();
        assertEquals(FetchTimingType.values().length * 3, map.size());
        for (FetchTimingType type : FetchTimingType.values()) {
            String key = type.toString();
            assertThat(map.get(key), greaterThan(0L));
            assertThat(map.get(key + TIMING_TYPE_COUNT_SUFFIX), equalTo(1L));
            assertThat(map.get(key + TIMING_TYPE_START_TIME_SUFFIX), greaterThan(0L));
        }
        long total = breakdown.toNodeTime();
        long sum = 0;
        for (FetchTimingType type : FetchTimingType.values()) {
            sum += map.get(type.toString());
        }
        assertEquals(sum, total);
    }
}
