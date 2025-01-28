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

import org.opensearch.search.profile.query.QueryTimingType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 *
 * @opensearch.internal
 */
public abstract class AbstractProfileBreakdown<T extends Enum<T>> {

    /**
     * The accumulated timings for this query node
     */
    protected final Map<String, Timer> timings;
    public static final String TIMING_TYPE_COUNT_SUFFIX = "_count";
    public static final String TIMING_TYPE_START_TIME_SUFFIX = "_start_time";

    /** Sole constructor. */
    public AbstractProfileBreakdown(final Class<T> timingType, final Set<String> additionalProfilerTimings) {
        Set<String> additionalTimings = additionalProfilerTimings == null ? Collections.emptySet() : additionalProfilerTimings;
        timings = Stream.of(Arrays.stream(timingType.getEnumConstants()).map(Enum::name),
                additionalTimings.stream())
            .flatMap(Function.identity())
            .filter(Objects::nonNull)
            .map(val -> val.toLowerCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableMap(value -> value, value -> new Timer(), (a, b) -> a));
    }

    public Timer getTimer(T timing) {
        return timings.get(timing.name().toLowerCase(Locale.ROOT));
    }

    public Timer getTimer(String timingName) {
        return timings.get(timingName.toLowerCase(Locale.ROOT));
    }

    public void setTimer(T timing, Timer timer) {
        timings.put(timing.name().toLowerCase(Locale.ROOT), timer);
    }

    /**
     * Build a timing count breakdown for current instance
     */
    public Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = new TreeMap<>();
        for (String timingType : this.timings.keySet()) {
            map.put(timingType, this.timings.get(timingType).getApproximateTiming());
            map.put(timingType + TIMING_TYPE_COUNT_SUFFIX, this.timings.get(timingType).getCount());
            map.put(timingType + TIMING_TYPE_START_TIME_SUFFIX, this.timings.get(timingType).getEarliestTimerStartTime());
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Fetch extra debugging information.
     */
    public Map<String, Object> toDebugMap() {
        return emptyMap();
    }

    public long toNodeTime() {
        long total = 0;
        for (String timingType : timings.keySet()) {
            total += timings.get(timingType).getApproximateTiming();
        }
        return total;
    }
}
