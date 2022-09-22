/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.action.admin.cluster.node.stats.RestActionsStats;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A service to fetch rest actions responses count
 *
 * @opensearch.internal
 */
public class RestActionsService {
    private final Map<String, Map<Integer, Integer>> handlers;

    public RestActionsService() {
        handlers = new ConcurrentHashMap<>();
    }

    public RestActionsStats stats(Set<String> restActionFilters) {
        Map<String, Map<Integer, Integer>> restActionStatusCount = getRestActionsStatusCount(restActionFilters);
        Map<Integer, Integer> restActionsStatusTotalCount = getRestActionsStatusTotalCount(restActionStatusCount);
        return new RestActionsStats(restActionStatusCount, restActionsStatusTotalCount);
    }

    private Map<Integer, Integer> getRestActionsStatusTotalCount(Map<String, Map<Integer, Integer>> mapHandlerStatusCount) {
        final Map<Integer, Integer> totalStatusCount = new TreeMap<>();
        mapHandlerStatusCount.entrySet().stream().flatMap(entry -> entry.getValue().entrySet().stream()).forEach(entry -> {
            Integer count = totalStatusCount.get(entry.getKey());
            if (null == count) count = 0;
            count += entry.getValue();
            totalStatusCount.put(entry.getKey(), count);
        });
        return totalStatusCount;
    }

    public void addRestHandler(BaseRestHandler handler) {
        Objects.requireNonNull(handler);
        if (handler.getName() == null) {
            throw new IllegalArgumentException("handler of type [" + handler.getClass().getName() + "] does not have a name");
        }
        handlers.putIfAbsent(handler.getName(), new TreeMap<>());
    }

    public void putStatus(String handlerName, RestStatus status) {
        if (handlers.containsKey(handlerName)) {
            increment(handlers.get(handlerName), status.getStatus());
        }
    }

    private void increment(Map<Integer, Integer> statusCount, int status) {
        Integer count = statusCount.get(status);
        if (null == count) count = 0;
        statusCount.put(status, ++count);
    }

    private Map<String, Map<Integer, Integer>> getRestActionsStatusCount(Set<String> restActionFilters) {
        if (null == restActionFilters || restActionFilters.isEmpty()) return handlers.entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 0)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Map<Integer, Integer>> restActionStatusCount = new ConcurrentHashMap<>();
        for (String action : restActionFilters) {
            Map<Integer, Integer> statusCount = handlers.get(action);
            if (null != statusCount && statusCount.size() > 0) {
                restActionStatusCount.put(action, statusCount);
            }
        }
        return Collections.unmodifiableMap(restActionStatusCount);
    }
}
