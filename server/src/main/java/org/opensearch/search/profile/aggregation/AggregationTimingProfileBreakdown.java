/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.aggregation;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.profile.AbstractTimingProfileBreakdown;
import org.opensearch.search.profile.Timer;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * A profile breakdown for aggregations.
 */
@PublicApi(since = "3.0.0")
public class AggregationTimingProfileBreakdown extends AbstractTimingProfileBreakdown<AggregationTimingType> {
    private final Map<String, Object> extra = new HashMap<>();

    public AggregationTimingProfileBreakdown() {
        for(AggregationTimingType type : AggregationTimingType.values()) {
            timers.put(type, new Timer());
        }
    }

    /**
     * Add extra debugging information about the aggregation.
     */
    public void addDebugInfo(String key, Object value) {
        extra.put(key, value);
    }

    @Override
    public Map<String, Object> toDebugMap() {
        return unmodifiableMap(extra);
    }
}
