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
import org.opensearch.ratelimitting.admissioncontrol.settings.IoBasedAdmissionControllerSettings;

import java.util.Locale;
import java.util.Optional;

/**
 *  Class for IO Based Admission Controller in OpenSearch, which aims to provide IO utilisation admission control.
 *  It provides methods to apply admission control if configured limit has been reached
 */
public class IoBasedAdmissionController extends AdmissionController {
    public static final String IO_BASED_ADMISSION_CONTROLLER = "global_io_usage";
    private static final Logger LOGGER = LogManager.getLogger(IoBasedAdmissionController.class);
    public IoBasedAdmissionControllerSettings settings;

    /**
     * @param admissionControllerName       name of the admissionController
     * @param resourceUsageCollectorService instance used to get resource usage stats of the node
     * @param clusterService                instance of the clusterService
     */
    public IoBasedAdmissionController(
        String admissionControllerName,
        ResourceUsageCollectorService resourceUsageCollectorService,
        ClusterService clusterService,
        Settings settings
    ) {
        super(admissionControllerName, resourceUsageCollectorService, clusterService);
        this.settings = new IoBasedAdmissionControllerSettings(clusterService.getClusterSettings(), settings);
    }

    /**
     * Apply admission control based on the resource usage for an action
     *
     * @param action is the transport action
     * @param admissionControlActionType type of admissionControlActionType
     */
    @Override
    public void apply(String action, AdmissionControlActionType admissionControlActionType) {
        if (this.isEnabledForTransportLayer(this.settings.getTransportLayerAdmissionControllerMode())) {
            this.applyForTransportLayer(action, admissionControlActionType);
        }
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
                        "IO usage admission controller rejected the request for action [%s] as IO limit reached for action-type [%s]",
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
            long ioUsageThreshold = this.getIoRejectionThreshold(admissionControlActionType);
            Optional<NodeResourceUsageStats> nodePerformanceStatistics = this.resourceUsageCollectorService.getNodeStatistics(
                this.clusterService.state().nodes().getLocalNodeId()
            );
            if (nodePerformanceStatistics.isPresent()) {
                double ioUsage = nodePerformanceStatistics.get().getIoUsageStats().getIoUtilisationPercent();
                if (ioUsage >= ioUsageThreshold) {
                    LOGGER.warn(
                        "IoBasedAdmissionController limit reached as the current IO "
                            + "usage [{}] exceeds the allowed limit [{}] for transport action [{}] in admissionControlMode [{}]",
                        ioUsage,
                        ioUsageThreshold,
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
     * Get IO rejection threshold based on action type
     */
    private long getIoRejectionThreshold(AdmissionControlActionType admissionControlActionType) {
        switch (admissionControlActionType) {
            case SEARCH:
                return this.settings.getSearchIOUsageLimit();
            case INDEXING:
                return this.settings.getIndexingIOUsageLimit();
            case CLUSTER_ADMIN:
                return this.settings.getClusterAdminIOUsageLimit();
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
