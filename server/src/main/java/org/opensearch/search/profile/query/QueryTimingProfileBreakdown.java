/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.AbstractTimingProfileBreakdown;
import org.opensearch.search.profile.Timer;

import java.util.List;
import java.util.Map;

/**
 * A {@link AbstractTimingProfileBreakdown} for query timings.
 */
public class QueryTimingProfileBreakdown extends AbstractTimingProfileBreakdown implements TimingProfileContext {

    private final AbstractTimingProfileBreakdown pluginBreakdown;

    public QueryTimingProfileBreakdown(AbstractTimingProfileBreakdown pluginBreakdown) {
        for(QueryTimingType type : QueryTimingType.values()) {
            timers.put(type.toString(), new Timer());
        }

        if(pluginBreakdown != null) timers.putAll(pluginBreakdown.getTimers());

        this.pluginBreakdown = pluginBreakdown;
    }

    public AbstractTimingProfileBreakdown getPluginBreakdown() {
        return pluginBreakdown;
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
}
