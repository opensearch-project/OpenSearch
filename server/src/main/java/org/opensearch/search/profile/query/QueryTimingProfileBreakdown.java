/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.AbstractTimingProfileBreakdown;
import org.opensearch.search.profile.Timer;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * A {@link AbstractTimingProfileBreakdown} for query timings.
 */
public class QueryTimingProfileBreakdown extends AbstractTimingProfileBreakdown implements TimingProfileContext {

    private final AbstractTimingProfileBreakdown pluginBreakdown;

    public QueryTimingProfileBreakdown(Class<? extends AbstractTimingProfileBreakdown> pluginBreakdownClass) {
        for(QueryTimingType type : QueryTimingType.values()) {
            timers.put(type.toString(), new Timer());
        }

        if (pluginBreakdownClass != null) {
            try {
                pluginBreakdown = pluginBreakdownClass.getDeclaredConstructor().newInstance();
                timers.putAll(pluginBreakdown.getTimers());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        else {
            pluginBreakdown = null;
        }
    }

    @Override
    public Map<String, Long> toImportantMetricsMap() {
        if(pluginBreakdown != null) return pluginBreakdown.toImportantMetricsMap();
        return Map.of();
    }

    @Override
    public Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = super.toBreakdownMap();
        if(pluginBreakdown != null) map.putAll(pluginBreakdown.toBreakdownMap());
        return map;

    }

    @Override
    public AbstractTimingProfileBreakdown context(Object context) {
        return this;
    }

    @Override
    public AbstractTimingProfileBreakdown getPluginBreakdown(Object context) {
        return pluginBreakdown;
    }
}
