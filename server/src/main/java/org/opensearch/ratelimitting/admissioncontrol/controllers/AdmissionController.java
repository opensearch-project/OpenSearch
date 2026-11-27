/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract class for Admission Controller in OpenSearch, which aims to provide resource based request admission control.
 * It provides methods for any tracking-object that can be incremented (such as memory size),
 * and admission control can be applied if configured limit has been reached
 */
public abstract class AdmissionController {
    private final String admissionControllerName;
    final ResourceUsageCollectorService resourceUsageCollectorService;
    public final Map<String, AtomicLong> rejectionCountMap;
    public final ClusterService clusterService;

    /**
     * @param admissionControllerName       name of the admissionController
     * @param resourceUsageCollectorService instance used to get resource usage stats of the node
     * @param clusterService instance of the clusterService
     */
    public AdmissionController(
        String admissionControllerName,
        ResourceUsageCollectorService resourceUsageCollectorService,
        ClusterService clusterService
    ) {
        this.admissionControllerName = admissionControllerName;
        this.resourceUsageCollectorService = resourceUsageCollectorService;
        this.clusterService = clusterService;
        this.rejectionCountMap = ConcurrentCollections.newConcurrentMap();
    }

    /**
     * Return the current state of the admission controller
     * @return true if admissionController is enabled for the transport layer else false
     */
    public boolean isEnabledForTransportLayer(AdmissionControlMode admissionControlMode) {
        return admissionControlMode != AdmissionControlMode.DISABLED;
    }

    /**
     *
     * @return true if admissionController is Enforced Mode else false
     */
    public Boolean isAdmissionControllerEnforced(AdmissionControlMode admissionControlMode) {
        return admissionControlMode == AdmissionControlMode.ENFORCED;
    }

    /**
     * Apply admission control based on the resource usage for an action
     */
    public abstract void apply(String action, AdmissionControlActionType admissionControlActionType);

    /**
     * @return name of the admission-controller
     */
    public String getName() {
        return this.admissionControllerName;
    }

    /**
     * Add rejection count to the rejection count metric tracked by the admission controller
     */
    public void addRejectionCount(String admissionControlActionType, long count) {
        // computeIfAbsent keeps the first increment atomic when concurrent requests are rejected for a new action type
        this.rejectionCountMap.computeIfAbsent(admissionControlActionType, k -> new AtomicLong(0)).getAndAdd(count);
    }

    /**
     * @return current value of the rejection count metric tracked by the admission-controller.
     */
    public long getRejectionCount(String admissionControlActionType) {
        // single lookup: no check-then-act window, and an action type that was never rejected reads as 0
        AtomicLong rejectionCount = this.rejectionCountMap.get(admissionControlActionType);
        return rejectionCount == null ? 0 : rejectionCount.get();
    }

    /**
     * Get rejection stats of the admission controller
     */
    public Map<String, Long> getRejectionStats() {
        Map<String, Long> rejectionStats = new HashMap<>();
        rejectionCountMap.forEach((actionType, count) -> rejectionStats.put(actionType, count.get()));
        return rejectionStats;
    }
}
