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
import org.opensearch.ratelimitting.admissioncontrol.settings.NativeMemoryBasedAdmissionControllerSettings;

import java.util.Locale;
import java.util.Optional;

/**
 * Class for Native Memory Based Admission Controller in OpenSearch, which aims to provide
 * admission control based on the actual physical memory usage on the node.
 * It provides methods to apply admission control if configured limit has been reached.
 */
public class NativeMemoryBasedAdmissionController extends AdmissionController {
    public static final String NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER = "global_native_memory_usage";
    private static final Logger LOGGER = LogManager.getLogger(NativeMemoryBasedAdmissionController.class);
    private NativeMemoryBasedAdmissionControllerSettings settings;

    /**
     * @param admissionControllerName       Name of the admission controller
     * @param resourceUsageCollectorService Instance used to get node resource usage stats
     * @param clusterService                ClusterService Instance
     * @param settings                      Immutable settings instance
     */
    public NativeMemoryBasedAdmissionController(
        String admissionControllerName,
        ResourceUsageCollectorService resourceUsageCollectorService,
        ClusterService clusterService,
        Settings settings
    ) {
        super(admissionControllerName, resourceUsageCollectorService, clusterService);
        this.settings = new NativeMemoryBasedAdmissionControllerSettings(clusterService.getClusterSettings(), settings);
    }

    /**
     * Apply admission control based on native memory usage
     * @param action is the transport action
     * @param admissionControlActionType type of admissionControlActionType
     */
    @Override
    public void apply(String action, AdmissionControlActionType admissionControlActionType) {
        if (this.isEnabledForTransportLayer(this.settings.getTransportLayerAdmissionControllerMode())) {
            this.applyForTransportLayer(action, admissionControlActionType);
        }
    }

    public NativeMemoryBasedAdmissionControllerSettings getSettings() {
        return this.settings;
    }

    /**
     * Apply transport layer admission control if configured limit has been reached
     */
    private void applyForTransportLayer(String actionName, AdmissionControlActionType admissionControlActionType) {
        if (isLimitsBreached(actionName, admissionControlActionType)) {
            this.addRejectionCount(admissionControlActionType.getType(), 1);
            if (this.isAdmissionControllerEnforced(this.settings.getTransportLayerAdmissionControllerMode())) {
                throw new OpenSearchRejectedExecutionException(
                    String.format(
                        Locale.ROOT,
                        "Memory usage admission controller rejected the request for action [%s] as native memory limit reached for action-type [%s]",
                        actionName,
                        admissionControlActionType.name()
                    )
                );
            }
        }
    }

    /**
     * Check if the configured resource usage limits are breached for the action
     */
    private boolean isLimitsBreached(String actionName, AdmissionControlActionType admissionControlActionType) {
        // check if cluster state is ready
        if (clusterService.state() != null && clusterService.state().nodes() != null) {
            long maxMemoryLimit = this.getMemoryRejectionThreshold(admissionControlActionType);
            Optional<NodeResourceUsageStats> nodePerformanceStatistics = this.resourceUsageCollectorService.getNodeStatistics(
                this.clusterService.state().nodes().getLocalNodeId()
            );
            if (nodePerformanceStatistics.isPresent()) {
                double memoryUsage = nodePerformanceStatistics.get().getNativeMemoryUtilizationPercent();
                if (memoryUsage >= maxMemoryLimit) {
                    LOGGER.warn(
                        "NativeMemoryBasedAdmissionController limit reached as the current native memory "
                            + "usage [{}] exceeds the allowed limit [{}] for transport action [{}] in admissionControlMode [{}]",
                        memoryUsage,
                        maxMemoryLimit,
                        actionName,
                        this.settings.getTransportLayerAdmissionControllerMode()
                    );
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get memory rejection threshold based on action type
     */
    private long getMemoryRejectionThreshold(AdmissionControlActionType admissionControlActionType) {
        switch (admissionControlActionType) {
            case SEARCH:
                return this.settings.getSearchNativeMemoryUsageLimit();
            case INDEXING:
                return this.settings.getIndexingNativeMemoryUsageLimit();
            case CLUSTER_ADMIN:
                return this.settings.getClusterAdminNativeMemoryUsageLimit();
            default:
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Admission control not Supported for AdmissionControlActionType: %s",
                        admissionControlActionType.getType()
                    )
                );
        }
    }
}
