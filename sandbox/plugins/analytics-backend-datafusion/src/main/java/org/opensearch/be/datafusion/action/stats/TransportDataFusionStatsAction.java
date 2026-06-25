/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.stats.AdaptiveBudgetStats;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats.OperationType;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.SpillStats;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transport action that fans out DataFusion stats requests to target nodes in the cluster.
 *
 * <p>Extends {@link TransportNodesAction} to leverage the standard node fan-out, failure handling,
 * and serialization infrastructure. On each target node, calls {@link DataFusionService#getStats()}
 * to obtain local stats and applies the stat section filter before returning.
 *
 * @opensearch.internal
 */
public class TransportDataFusionStatsAction extends TransportNodesAction<
    DataFusionStatsNodesRequest,
    DataFusionStatsNodesResponse,
    DataFusionStatsNodeRequest,
    DataFusionStatsNodeResponse> {

    private final DataFusionService dataFusionService;

    @Inject
    public TransportDataFusionStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DataFusionService dataFusionService
    ) {
        super(
            DataFusionStatsActionType.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            DataFusionStatsNodesRequest::new,
            DataFusionStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            DataFusionStatsNodeResponse.class
        );
        this.dataFusionService = dataFusionService;
    }

    @Override
    protected DataFusionStatsNodesResponse newResponse(
        DataFusionStatsNodesRequest request,
        List<DataFusionStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new DataFusionStatsNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected DataFusionStatsNodeRequest newNodeRequest(DataFusionStatsNodesRequest request) {
        return new DataFusionStatsNodeRequest(request);
    }

    @Override
    protected DataFusionStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new DataFusionStatsNodeResponse(in);
    }

    @Override
    protected DataFusionStatsNodeResponse nodeOperation(DataFusionStatsNodeRequest request) {
        DataFusionStats stats;
        try {
            stats = dataFusionService.getStats();
        } catch (IllegalStateException e) {
            // DataFusionService not started on this node — return null stats
            stats = null;
        }
        return new DataFusionStatsNodeResponse(clusterService.localNode(), filteredStats(stats, request.getStatsToRetrieve()));
    }

    /**
     * Applies the stat section filter to the given stats.
     *
     * <p>When the filter is null or empty, returns the full stats unchanged.
     * When a filter is present, constructs a new {@link DataFusionStats} instance
     * containing only the requested sections; sections not in the filter are nulled out.
     *
     * <p>Section name to source field mapping:
     * <ul>
     *   <li>{@code io_runtime} &rarr; {@link NativeExecutorsStats#getIoRuntime()}</li>
     *   <li>{@code cpu_runtime} &rarr; {@link NativeExecutorsStats#getCpuRuntime()}</li>
     *   <li>{@code coordinator_reduce} &rarr; {@link NativeExecutorsStats#getTaskMonitors()}.get("coordinator_reduce")</li>
     *   <li>{@code query_execution} &rarr; {@link NativeExecutorsStats#getTaskMonitors()}.get("query_execution")</li>
     *   <li>{@code stream_next} &rarr; {@link NativeExecutorsStats#getTaskMonitors()}.get("stream_next")</li>
     *   <li>{@code plan_setup} &rarr; {@link NativeExecutorsStats#getTaskMonitors()}.get("plan_setup")</li>
     *   <li>{@code fragment_executor_gate} &rarr; {@link DataFusionStats#getFragmentExecutorGateStats()}</li>
     *   <li>{@code adaptive_budget} &rarr; {@link DataFusionStats#getAdaptiveBudgetStats()}</li>
     *   <li>{@code spill} &rarr; {@link DataFusionStats#getSpillStats()}</li>
     * </ul>
     *
     * @param stats  the full stats (may be null)
     * @param filter the set of stat section names to include (null or empty means all)
     * @return the filtered stats, or the original stats if no filter is applied
     */
    static DataFusionStats filteredStats(DataFusionStats stats, Set<String> filter) {
        if (stats == null) {
            return null;
        }
        if (filter == null || filter.isEmpty()) {
            return stats;
        }

        // Determine which gate stats to include
        PartitionGateStats fragmentExecutorGate = filter.contains("fragment_executor_gate") ? stats.getFragmentExecutorGateStats() : null;

        // Determine which budget stats to include
        AdaptiveBudgetStats adaptiveBudget = filter.contains("adaptive_budget") ? stats.getAdaptiveBudgetStats() : null;

        // Determine which spill stats to include
        SpillStats spillStats = filter.contains("disk_spill") ? stats.getSpillStats() : null;

        // Determine which NativeExecutorsStats sections to include
        NativeExecutorsStats nativeStats = stats.getNativeExecutorsStats();
        NativeExecutorsStats filteredNativeStats = null;

        if (nativeStats != null) {
            boolean needsIoRuntime = filter.contains("io_runtime");
            boolean needsCpuRuntime = filter.contains("cpu_runtime");
            boolean needsAnyTaskMonitor = filter.contains("coordinator_reduce")
                || filter.contains("query_execution")
                || filter.contains("stream_next")
                || filter.contains("plan_setup");

            if (needsIoRuntime || needsCpuRuntime || needsAnyTaskMonitor) {
                RuntimeMetrics ioRuntime = needsIoRuntime ? nativeStats.getIoRuntime() : null;
                RuntimeMetrics cpuRuntime = needsCpuRuntime ? nativeStats.getCpuRuntime() : null;

                // Build filtered task monitors map — only include requested operation types
                Map<String, TaskMonitorStats> filteredMonitors = new LinkedHashMap<>();
                for (OperationType opType : OperationType.values()) {
                    if (filter.contains(opType.key())) {
                        TaskMonitorStats monitor = nativeStats.getTaskMonitors().get(opType.key());
                        if (monitor != null) {
                            filteredMonitors.put(opType.key(), monitor);
                        }
                    }
                }

                filteredNativeStats = new NativeExecutorsStats(ioRuntime, cpuRuntime, filteredMonitors);
            }
        }

        return new DataFusionStats(filteredNativeStats, fragmentExecutorGate, adaptiveBudget, spillStats);
    }
}
