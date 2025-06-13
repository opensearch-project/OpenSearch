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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 *
 * @opensearch.internal
 */
@PublicApi(since="3.0.0")
public abstract class AbstractProfileBreakdown {

    /** Sole constructor. */
    public AbstractProfileBreakdown() {}

    /**
     * Gather important metrics for current instance
     */
    abstract public Map<String, Long> toImportantMetricsMap();

    /**
     * Build a breakdown for current instance
     */
    abstract public Map<String, Long> toBreakdownMap();

    /**
     * Fetch extra debugging information.
     */
    public Map<String, Object> toDebugMap() {
        return emptyMap();
    }

    /**
     *
     * @return a {@link BiFunction} that handles the concurrent plugin metric for the profiler
     */
    public BiFunction<String, Long, Long> handleConcurrentPluginMetric() {
        throw new IllegalCallerException("must be overridden by plugin");
    }

    public Map<String, Long> filterZeros(Map<String, Long> map) {
        Map<String, Long> filteredMap = new HashMap<>(map.size());
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (entry.getValue() != 0) {
                filteredMap.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(filteredMap);
    }
}
