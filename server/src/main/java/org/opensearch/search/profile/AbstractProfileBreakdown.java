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

import org.opensearch.common.annotation.PublicApi;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 *
 * @opensearch.api
 */
@PublicApi(since = "3.2.0")
public abstract class AbstractProfileBreakdown {

    protected final Map<String, ProfileMetric> metrics;

    /** Sole constructor. */
    public AbstractProfileBreakdown(Collection<Supplier<ProfileMetric>> metricSuppliers) {
        this.metrics = metricSuppliers.stream().map(Supplier::get).collect(Collectors.toMap(ProfileMetric::getName, metric -> metric));
    }

    public Timer getTimer(Enum<?> type) {
        ProfileMetric metric = metrics.get(type.toString());
        assert metric instanceof Timer : "Metric " + type + " is not a timer";
        return (Timer) metric;
    }

    public ProfileMetric getMetric(String name) {
        return metrics.get(name);
    }

    /**
     * Build a breakdown for current instance
     */
    public Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = new TreeMap<>();
        for (Map.Entry<String, ProfileMetric> entry : metrics.entrySet()) {
            map.putAll(entry.getValue().toBreakdownMap());
        }
        return Collections.unmodifiableMap(map);
    }

    public long toNodeTime() {
        long total = 0;
        for (Map.Entry<String, ProfileMetric> entry : metrics.entrySet()) {
            if (entry.getValue() instanceof Timer t) {
                total += t.getApproximateTiming();
            }
        }
        return total;
    }

    /**
     * Fetch extra debugging information.
     */
    public Map<String, Object> toDebugMap() {
        return emptyMap();
    }

}
