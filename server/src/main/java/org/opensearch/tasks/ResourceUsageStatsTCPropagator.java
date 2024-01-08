/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.common.util.concurrent.ThreadContextStatePropagator;

public class ResourceUsageStatsTCPropagator implements ThreadContextStatePropagator  {
    public static final String NODE_RESOURCE_STATS = "PERF_STATS";
    @Override
    public Map<String, Object> transients(Map<String, Object> source) {
        final Map<String, Object> transients = new HashMap<>();
        for(Map.Entry<String, Object> entry : source.entrySet()) {
            if(entry.getKey().startsWith(NODE_RESOURCE_STATS)) {
                // key starts with prefix
                transients.put(entry.getKey(), entry.getValue());
            }
        }
        return transients;
    }

    @Override
    public Map<String, String> headers(Map<String, Object> source) {
        final Map<String, String> headers = new HashMap<>();
        for(Map.Entry<String, Object> entry : source.entrySet()) {
            if(entry.getKey().startsWith(NODE_RESOURCE_STATS)) {
                // key starts with prefix
                headers.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return headers;
    }
}
