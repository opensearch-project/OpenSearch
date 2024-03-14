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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search.aggregations.support;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.service.ReportingService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Service to track telemetry about aggregations
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AggregationUsageService implements ReportingService<AggregationInfo> {
    private final Map<String, Map<String, LongAdder>> aggs;
    private final AggregationInfo info;

    public static final String OTHER_SUBTYPE = "other";

    /**
     * Builder for the Agg usage service to track telemetry
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {
        private final Map<String, Map<String, LongAdder>> aggs;

        public Builder() {
            aggs = new HashMap<>();
        }

        public void registerAggregationUsage(String aggregationName) {
            registerAggregationUsage(aggregationName, OTHER_SUBTYPE);
        }

        public void registerAggregationUsage(String aggregationName, String valuesSourceType) {
            Map<String, LongAdder> subAgg = aggs.computeIfAbsent(aggregationName, k -> new HashMap<>());
            if (subAgg.put(valuesSourceType, new LongAdder()) != null) {
                throw new IllegalArgumentException(
                    "stats for aggregation [" + aggregationName + "][" + valuesSourceType + "] already registered"
                );
            }
        }

        public AggregationUsageService build() {
            return new AggregationUsageService(this);
        }
    }

    private AggregationUsageService(Builder builder) {
        this.aggs = builder.aggs;
        info = new AggregationInfo(aggs);
    }

    public void incAggregationUsage(String aggregationName, String valuesSourceType) {
        Map<String, LongAdder> valuesSourceMap = aggs.get(aggregationName);
        // Not all aggs register their usage at the moment we also don't register them in test context
        if (valuesSourceMap != null) {
            LongAdder adder = valuesSourceMap.get(valuesSourceType);
            if (adder != null) {
                adder.increment();
            }
            assert adder != null : "Unknown subtype [" + aggregationName + "][" + valuesSourceType + "]";
        }
        assert valuesSourceMap != null : "Unknown aggregation [" + aggregationName + "][" + valuesSourceType + "]";
    }

    public Map<String, Object> getUsageStats() {
        Map<String, Object> aggsUsageMap = new HashMap<>();
        aggs.forEach((name, agg) -> {
            Map<String, Long> aggUsageMap = new HashMap<>();
            agg.forEach((k, v) -> {
                long val = v.longValue();
                if (val > 0) {
                    aggUsageMap.put(k, val);
                }
            });
            if (aggUsageMap.isEmpty() == false) {
                aggsUsageMap.put(name, aggUsageMap);
            }
        });
        return aggsUsageMap;
    }

    @Override
    public AggregationInfo info() {
        return info;
    }
}
