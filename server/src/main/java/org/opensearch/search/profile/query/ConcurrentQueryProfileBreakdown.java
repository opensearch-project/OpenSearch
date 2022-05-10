/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.ContextualProfileBreakdown;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc). The class supports profiling the concurrent search over segments.
 *
 * @opensearch.internal
 */
public final class ConcurrentQueryProfileBreakdown extends ContextualProfileBreakdown<QueryTimingType> {
    private final Map<Object, AbstractProfileBreakdown<QueryTimingType>> contexts = new ConcurrentHashMap<>();

    /** Sole constructor. */
    public ConcurrentQueryProfileBreakdown() {
        super(QueryTimingType.class);
    }

    @Override
    public AbstractProfileBreakdown<QueryTimingType> context(Object context) {
        // See please https://bugs.openjdk.java.net/browse/JDK-8161372
        final AbstractProfileBreakdown<QueryTimingType> profile = contexts.get(context);

        if (profile != null) {
            return profile;
        }

        return contexts.computeIfAbsent(context, ctx -> new QueryProfileBreakdown());
    }

    @Override
    public Map<String, Long> toBreakdownMap() {
        final Map<String, Long> map = new HashMap<>(buildBreakdownMap(this));

        for (final AbstractProfileBreakdown<QueryTimingType> context : contexts.values()) {
            for (final Map.Entry<String, Long> entry : buildBreakdownMap(context).entrySet()) {
                map.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
        }

        return map;
    }
}
