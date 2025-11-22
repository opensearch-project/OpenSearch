/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.aggregation.startree;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.opensearch.search.profile.Timer.TIMING_TYPE_COUNT_SUFFIX;
import static org.opensearch.search.profile.Timer.TIMING_TYPE_START_TIME_SUFFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StarTreeProfileBreakdownTests extends OpenSearchTestCase {
    public void testBreakdownMap() {
        StarTreeProfileBreakdown breakdown = new StarTreeProfileBreakdown();
        for (StarTreeAggregationTimingType type : StarTreeAggregationTimingType.values()) {
            breakdown.getTimer(type).start();
            breakdown.getTimer(type).stop();
        }
        Map<String, Long> map = breakdown.toBreakdownMap();
        assertEquals(StarTreeAggregationTimingType.values().length * 3, map.size());
        for (StarTreeAggregationTimingType type : StarTreeAggregationTimingType.values()) {
            String key = type.toString();
            assertThat(map.get(key), greaterThan(0L));
            assertThat(map.get(key + TIMING_TYPE_COUNT_SUFFIX), equalTo(1L));
            assertThat(map.get(key + TIMING_TYPE_START_TIME_SUFFIX), greaterThan(0L));
        }
        long total = breakdown.toNodeTime();
        long sum = 0;
        for (StarTreeAggregationTimingType type : StarTreeAggregationTimingType.values()) {
            sum += map.get(type.toString());
        }
        assertEquals(sum, total);
    }
}
