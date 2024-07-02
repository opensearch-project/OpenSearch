/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.util.concurrent.ThreadContextStatePropagator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to propagate QueryGroup related headers to request and nodes
 */
public class QueryGroupThreadContextStatePropagator implements ThreadContextStatePropagator {
    // TODO: move this constant to QueryGroupService class once the QueryGroup monitoring framework PR is ready
    public static List<String> PROPAGATED_HEADERS = List.of("queryGroupId");

    /**
     * @param source current context transient headers
     * @return the map of header and their values to be propagated across request threadContexts
     */
    @Override
    public Map<String, Object> transients(Map<String, Object> source) {
        final Map<String, Object> transientHeaders = new HashMap<>();

        for (String headerName : PROPAGATED_HEADERS) {
            if (source.containsKey(headerName)) {
                transientHeaders.put(headerName, source.get(headerName));
            }
        }
        return transientHeaders;
    }

    /**
     * @param source current context headers
     * @return map of header and their values to be propagated across nodes
     */
    @Override
    public Map<String, String> headers(Map<String, Object> source) {
        final Map<String, String> propagatedHeaders = new HashMap<>();

        for (String headerName : PROPAGATED_HEADERS) {
            if (source.containsKey(headerName)) {
                propagatedHeaders.put(headerName, (String) source.get(headerName));
            }
        }
        return propagatedHeaders;
    }
}
