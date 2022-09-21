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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A service to fetch rest actions responses count
 *
 * @opensearch.internal
 */
public class RestActionsStatusCountService {
    /**
     * Map that contains mapping of action and all response status and count
     */
    private final Map<String, Map<Integer, AtomicLong>> handlers;

    public RestActionsStatusCountService() {
        handlers = new ConcurrentHashMap<>();
    }

    public RestActionsStats stats(Set<String> restActionFilters) {
        Map<String, Map<Integer, AtomicLong>> restActionStatusCount = getRestActionsStatusCount(restActionFilters);
        Map<Integer, AtomicLong> restActionsStatusTotalCount = getRestActionsStatusTotalCount(restActionStatusCount);
        return new RestActionsStats(restActionStatusCount, restActionsStatusTotalCount);
    }

    private Map<Integer, AtomicLong> getRestActionsStatusTotalCount(Map<String, Map<Integer, AtomicLong>> restActionStatusCount) {
        final Map<Integer, AtomicLong> totalStatusCount = new TreeMap<>();
        totalStatusCount.put(RestStatus.OK.getStatus(), new AtomicLong(0));
        restActionStatusCount.entrySet().stream().flatMap(entry -> entry.getValue().entrySet().stream()).forEach(entry -> {
            if (!totalStatusCount.containsKey(entry.getKey())) {
                synchronized (totalStatusCount) {
                    if (!totalStatusCount.containsKey(entry.getKey())) {
                        totalStatusCount.put(entry.getKey(), new AtomicLong(0));
                    }
                }
            }
            totalStatusCount.get(entry.getKey()).addAndGet(entry.getValue().longValue());
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

    private void increment(Map<Integer, AtomicLong> statusCount, int status) {
        if (!statusCount.containsKey(status)) {
            synchronized (this) {
                if (!statusCount.containsKey(status)) {
                    statusCount.put(status, new AtomicLong(0));
                }
            }
        }
        statusCount.get(status).incrementAndGet();
    }

    private Map<String, Map<Integer, AtomicLong>> getRestActionsStatusCount(Set<String> restActionFilters) {
        if (null == restActionFilters || restActionFilters.isEmpty()) return handlers.entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 0)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Map<Integer, AtomicLong>> restActionStatusCount = new ConcurrentHashMap<>();
        for (String action : restActionFilters) {
            Map<Integer, AtomicLong> statusCount = handlers.get(action);
            if (null != statusCount && statusCount.size() > 0) {
                restActionStatusCount.put(action, statusCount);
            }
        }
        return Collections.unmodifiableMap(restActionStatusCount);
    }

    public Map<String, Map<Integer, AtomicLong>> getHandlers() {
        return Collections.unmodifiableMap(handlers);
    }
}
