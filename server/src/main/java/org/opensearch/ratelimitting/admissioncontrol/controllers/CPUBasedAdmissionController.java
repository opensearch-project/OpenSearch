/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CPUBasedAdmissionControllerSettings;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  Class for CPU Based Admission Controller in OpenSearch, which aims to provide CPU utilisation admission control.
 *  It provides methods to apply admission control if configured limit has been reached
 */
public class CPUBasedAdmissionController extends AdmissionController {
    private static final Logger LOGGER = LogManager.getLogger(CPUBasedAdmissionController.class);
    public CPUBasedAdmissionControllerSettings settings;

    /**
     *
     * @param admissionControllerName State of the admission controller
     */
    public CPUBasedAdmissionController(String admissionControllerName, Settings settings, ClusterService clusterService, ResourceUsageCollectorService resourceUsageCollectorService) {
        super(new AtomicLong(0), admissionControllerName, resourceUsageCollectorService, clusterService);
        this.settings = new CPUBasedAdmissionControllerSettings(clusterService.getClusterSettings(), settings);
    }

    /**
     * This function will take of applying admission controller based on CPU usage
     * @param action is the transport action
     */
    @Override
    public void apply(String action, AdmissionControlActionType admissionControlActionType) {
        // TODO Will extend this logic further currently just incrementing rejectionCount
        if (this.isEnabledForTransportLayer(this.settings.getTransportLayerAdmissionControllerMode())) {
            this.applyForTransportLayer(action, admissionControlActionType);
        }
    }

    private void applyForTransportLayer(String actionName, AdmissionControlActionType admissionControlActionType) {
        if (isLimitsBreached(admissionControlActionType)) {
            this.addRejectionCount(admissionControlActionType.getType(), 1);
            if (this.isAdmissionControllerEnforced(this.settings.getTransportLayerAdmissionControllerMode())) {
                throw new OpenSearchRejectedExecutionException("Action ["+ actionName +"] was rejected due to CPU usage admission controller limit breached");
            }
        }
    }

    private boolean isLimitsBreached(AdmissionControlActionType transportActionType) {
        long maxCpuLimit = this.getCpuRejectionThreshold(transportActionType);
        Optional<NodeResourceUsageStats> nodePerformanceStatistics = this.resourceUsageCollectorService.getNodeStatistics(this.clusterService.state().nodes().getLocalNodeId());
        if(nodePerformanceStatistics.isPresent()) {
            double cpuUsage = nodePerformanceStatistics.get().getCpuUtilizationPercent();
            if (cpuUsage >= maxCpuLimit){
                LOGGER.warn("CpuBasedAdmissionController rejected the request as the current CPU usage [" +
                    cpuUsage + "%] exceeds the allowed limit [" + maxCpuLimit + "%]");
                return true;
            }
        }
        return false;
    }
    private long getCpuRejectionThreshold(AdmissionControlActionType transportActionType) {
        switch (transportActionType) {
            case SEARCH:
                return this.settings.getSearchCPULimit();
            case INDEXING:
                return this.settings.getIndexingCPULimit();
            default:
                throw new IllegalArgumentException("Not Supported TransportAction Type: " + transportActionType.getType());
        }
    }
}
