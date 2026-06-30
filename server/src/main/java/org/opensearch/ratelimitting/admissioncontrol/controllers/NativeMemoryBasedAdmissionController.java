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
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.settings.NativeMemoryBasedAdmissionControllerSettings;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Class for Native Memory Based Admission Controller in OpenSearch, which aims to provide
 * admission control based on the actual physical memory usage on the node.
 * It provides methods to apply admission control if configured limit has been reached.
 */
public class NativeMemoryBasedAdmissionController extends AdmissionController {
    public static final String NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER = "global_native_memory_usage";
    private static final Logger LOGGER = LogManager.getLogger(NativeMemoryBasedAdmissionController.class);

    /**
     * Refresh interval for the cached indexing-pool utilization snapshot. The pool stats supplier is
     * sampled at most once per interval so the per-request admission path only reads a cached value.
     */
    private static final TimeValue INDEXING_POOL_STATS_REFRESH_INTERVAL = TimeValue.timeValueSeconds(1);

    private NativeMemoryBasedAdmissionControllerSettings settings;

    /**
     * Nullable supplier of native allocator pool stats, installed by a plugin (today: ArrowBasePlugin).
     * When {@code null} the indexing-pool admission check is skipped entirely and behavior is unchanged.
     */
    private final Supplier<NativeAllocatorPoolStats> nativeAllocatorStatsSupplier;

    /**
     * Cached indexing-pool utilization percentage ({@code allocated / limit * 100}), or {@code null} when
     * no stats supplier is installed. A value of {@code -1.0} means the signal is currently unavailable.
     */
    private final SingleObjectCache<Double> indexingPoolUtilizationCache;

    /**
     * @param admissionControllerName       Name of the admission controller
     * @param resourceUsageCollectorService Instance used to get node resource usage stats
     * @param clusterService                ClusterService Instance
     * @param settings                      Immutable settings instance
     * @param nativeAllocatorStatsSupplier  Nullable supplier of native allocator pool stats; when {@code null}
     *                                      the indexing-pool admission check is disabled
     */
    public NativeMemoryBasedAdmissionController(
        String admissionControllerName,
        ResourceUsageCollectorService resourceUsageCollectorService,
        ClusterService clusterService,
        Settings settings,
        Supplier<NativeAllocatorPoolStats> nativeAllocatorStatsSupplier
    ) {
        super(admissionControllerName, resourceUsageCollectorService, clusterService);
        this.settings = new NativeMemoryBasedAdmissionControllerSettings(clusterService.getClusterSettings(), settings);
        this.nativeAllocatorStatsSupplier = nativeAllocatorStatsSupplier;
        this.indexingPoolUtilizationCache = nativeAllocatorStatsSupplier == null
            ? null
            : new SingleObjectCache<>(INDEXING_POOL_STATS_REFRESH_INTERVAL, -1.0) {
                @Override
                protected Double refresh() {
                    return computeIndexingPoolUtilizationPercent();
                }
            };
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
        // Indexing-pool based check: reject indexing requests when the INDEXING native memory pool
        // utilization breaches its configured limit. Skipped entirely when no stats supplier is installed.
        if (admissionControlActionType == AdmissionControlActionType.INDEXING && this.indexingPoolUtilizationCache != null) {
            double indexingPoolUsage = this.indexingPoolUtilizationCache.getOrRefresh();
            long indexingPoolLimit = this.settings.getIndexingNativeMemoryPoolUsageLimit();
            if (indexingPoolUsage >= 0.0 && indexingPoolUsage >= indexingPoolLimit) {
                LOGGER.warn(
                    "NativeMemoryBasedAdmissionController limit reached as the current indexing native memory pool "
                        + "usage [{}] exceeds the allowed limit [{}] for transport action [{}] in admissionControlMode [{}]",
                    indexingPoolUsage,
                    indexingPoolLimit,
                    actionName,
                    this.settings.getTransportLayerAdmissionControllerMode()
                );
                return true;
            }
        }
        return false;
    }

    /**
     * Computes the current utilization percentage of the INDEXING native memory pool group as
     * {@code allocated / limit * 100}. Returns {@code -1.0} (signal unavailable) when the supplier is
     * absent, returns {@code null}, the INDEXING group is missing, or the pool limit is non-positive.
     */
    private double computeIndexingPoolUtilizationPercent() {
        Supplier<NativeAllocatorPoolStats> supplier = this.nativeAllocatorStatsSupplier;
        if (supplier == null) {
            return -1.0;
        }
        NativeAllocatorPoolStats stats;
        try {
            stats = supplier.get();
        } catch (RuntimeException e) {
            LOGGER.debug("native allocator pool stats supplier threw; skipping indexing-pool admission check", e);
            return -1.0;
        }
        if (stats == null) {
            return -1.0;
        }
        NativeAllocatorPoolStats.PoolStats indexingGroup = stats.getGroupedStats().get(PoolGroup.INDEXING.getName());
        if (indexingGroup == null || indexingGroup.getLimitBytes() <= 0) {
            return -1.0;
        }
        return 100.0 * indexingGroup.getAllocatedBytes() / indexingGroup.getLimitBytes();
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
