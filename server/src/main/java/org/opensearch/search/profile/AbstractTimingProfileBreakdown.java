/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.common.annotation.PublicApi;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all timing profile breakdowns.
 */
@PublicApi(since="3.0.0")
public abstract class AbstractTimingProfileBreakdown extends AbstractProfileBreakdown {

    protected final Map<String, Timer> timers = new HashMap<>();
    public static final String TIMING_TYPE_COUNT_SUFFIX = "_count";
    public static final String TIMING_TYPE_START_TIME_SUFFIX = "_start_time";
    public static final String NODE_TIME_RAW = "time_in_nanos";

    public AbstractTimingProfileBreakdown() {}

    public Timer getTimer(String type) {
        return timers.get(type);
    }

    public Map<String, Timer> getTimers() {
        return timers;
    }

    public long toNodeTime() {
        long total = 0;
        for(Timer timer : timers.values()) {
            total += timer.getApproximateTiming();
        }
        return total;
    }

    /**
     * Build a timing count breakdown for current instance
     */
    @Override
    public Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = new HashMap<>(timers.size() * 3);
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getApproximateTiming());
            map.put(entry.getKey() + TIMING_TYPE_COUNT_SUFFIX, entry.getValue().getCount());
            map.put(entry.getKey() + TIMING_TYPE_START_TIME_SUFFIX, entry.getValue().getEarliestTimerStartTime());
        }
        map.put(NODE_TIME_RAW, toNodeTime());
        return map;
    }
}
