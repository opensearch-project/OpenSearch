/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.search.profile.AbstractProfileBreakdown;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FetchProfileBreakdown extends AbstractProfileBreakdown<FetchTimingType> {
    public FetchProfileBreakdown() {
        super(FetchTimingType.class);
    }

    @Override
    public Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = super.toBreakdownMap();
        Map<String, Long> filtered = new HashMap<>(map.size());
        for (FetchTimingType type : FetchTimingType.values()) {
            String key = type.toString();
            long count = map.get(key + TIMING_TYPE_COUNT_SUFFIX);
            long time = map.get(key);
            long start = map.get(key + TIMING_TYPE_START_TIME_SUFFIX);
            if (count > 0 || time > 0) {
                filtered.put(key, time);
                filtered.put(key + TIMING_TYPE_COUNT_SUFFIX, count);
                filtered.put(key + TIMING_TYPE_START_TIME_SUFFIX, start);
            }
        }
        return Collections.unmodifiableMap(filtered);
    }
}
