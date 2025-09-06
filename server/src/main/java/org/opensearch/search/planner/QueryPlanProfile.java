/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.HashMap;
import java.util.Map;

/**
 * Profiling information for a query plan node.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class QueryPlanProfile {

    private final String queryType;
    private final QueryCost estimatedCost;
    private final Map<String, Object> attributes;

    public QueryPlanProfile(String queryType, QueryCost estimatedCost) {
        this.queryType = queryType;
        this.estimatedCost = estimatedCost;
        this.attributes = new HashMap<>();
    }

    public void addAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public String getQueryType() {
        return queryType;
    }

    public QueryCost getEstimatedCost() {
        return estimatedCost;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("type", queryType);
        map.put("cost", costToMap());
        if (!attributes.isEmpty()) {
            map.put("attributes", attributes);
        }
        return map;
    }

    private Map<String, Object> costToMap() {
        Map<String, Object> costMap = new HashMap<>();
        costMap.put("lucene_docs", estimatedCost.getLuceneCost());
        costMap.put("cpu", estimatedCost.getCpuCost());
        costMap.put("memory_bytes", estimatedCost.getMemoryCost());
        costMap.put("io", estimatedCost.getIoCost());
        costMap.put("total", estimatedCost.getTotalCost());
        costMap.put("is_estimate", estimatedCost.isEstimate());
        return costMap;
    }
}
