/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all timing profile breakdowns.
 */
public abstract class AbstractTimingProfileBreakdown<T extends Enum<T>> extends AbstractProfileBreakdown<T> {

    protected final Map<T, Timer> timers = new HashMap<>();
    public static final String TIMING_TYPE_COUNT_SUFFIX = "_count";
    public static final String TIMING_TYPE_START_TIME_SUFFIX = "_start_time";

    public AbstractTimingProfileBreakdown() {}

    public Timer getTimer(T type) {
        return timers.get(type);
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
        for (Map.Entry<T, Timer> entry : timers.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().getApproximateTiming());
            map.put(entry.getKey() + TIMING_TYPE_COUNT_SUFFIX, entry.getValue().getCount());
            map.put(entry.getKey() + TIMING_TYPE_START_TIME_SUFFIX, entry.getValue().getEarliestTimerStartTime());
        }
        return Collections.unmodifiableMap(map);
    }
}
