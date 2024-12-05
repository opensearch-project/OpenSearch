/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.Metric;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.seqno.RetentionLeaseStats;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.NodeService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transport action for obtaining cluster state
 *
 * @opensearch.internal
 */
public class TransportClusterStatsAction extends TransportNodesAction<
    ClusterStatsRequest,
    ClusterStatsResponse,
    TransportClusterStatsAction.ClusterStatsNodeRequest,
    ClusterStatsNodeResponse> {

    private static final Map<CommonStatsFlags.Flag, ClusterStatsRequest.IndexMetric> SHARDS_STATS_FLAG_MAP_TO_INDEX_METRIC = Map.of(
        CommonStatsFlags.Flag.Docs,
        ClusterStatsRequest.IndexMetric.DOCS,
        CommonStatsFlags.Flag.Store,
        ClusterStatsRequest.IndexMetric.STORE,
        CommonStatsFlags.Flag.FieldData,
        ClusterStatsRequest.IndexMetric.FIELDDATA,
        CommonStatsFlags.Flag.QueryCache,
        ClusterStatsRequest.IndexMetric.QUERY_CACHE,
        CommonStatsFlags.Flag.Completion,
        ClusterStatsRequest.IndexMetric.COMPLETION,
        CommonStatsFlags.Flag.Segments,
        ClusterStatsRequest.IndexMetric.SEGMENTS
    );

    private final NodeService nodeService;
    private final IndicesService indicesService;

    @Inject
    public TransportClusterStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(
            ClusterStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClusterStatsRequest::new,
            ClusterStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ThreadPool.Names.MANAGEMENT,
            ClusterStatsNodeResponse.class
        );
        this.nodeService = nodeService;
        this.indicesService = indicesService;
    }

    @Override
    protected ClusterStatsResponse newResponse(
        ClusterStatsRequest request,
        List<ClusterStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        assert Transports.assertNotTransportThread(
            "Constructor of ClusterStatsResponse runs expensive computations on mappings found in"
                + " the cluster state that are too slow for a transport thread"
        );
        ClusterState state = clusterService.state();
        if (request.computeAllMetrics()) {
            return new ClusterStatsResponse(
                System.currentTimeMillis(),
                state.metadata().clusterUUID(),
                clusterService.getClusterName(),
                responses,
                failures,
                state
            );
        } else {
            return new ClusterStatsResponse(
                System.currentTimeMillis(),
                state.metadata().clusterUUID(),
                clusterService.getClusterName(),
                responses,
                failures,
                state,
                request.requestedMetrics(),
                request.indicesMetrics()
            );
        }
    }

    @Override
    protected ClusterStatsNodeRequest newNodeRequest(ClusterStatsRequest request) {
        return new ClusterStatsNodeRequest(request);
    }

    @Override
    protected ClusterStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    @Override
    protected ClusterStatsNodeResponse nodeOperation(ClusterStatsNodeRequest nodeRequest) {
        NodeInfo nodeInfo = nodeService.info(true, true, false, true, false, true, false, true, false, false, false, false);
        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            isMetricRequired(Metric.OS, nodeRequest.request),
            isMetricRequired(Metric.PROCESS, nodeRequest.request),
            isMetricRequired(Metric.JVM, nodeRequest.request),
            false,
            isMetricRequired(Metric.FS, nodeRequest.request),
            false,
            false,
            false,
            false,
            false,
            isMetricRequired(Metric.INGEST, nodeRequest.request),
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false
        );
        List<ShardStats> shardsStats = new ArrayList<>();
        if (isMetricRequired(Metric.INDICES, nodeRequest.request)) {
            CommonStatsFlags commonStatsFlags = getCommonStatsFlags(nodeRequest);
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    if (indexShard.routingEntry() != null && indexShard.routingEntry().active()) {
                        // only report on fully started shards
                        CommitStats commitStats;
                        SeqNoStats seqNoStats;
                        RetentionLeaseStats retentionLeaseStats;
                        try {
                            commitStats = indexShard.commitStats();
                            seqNoStats = indexShard.seqNoStats();
                            retentionLeaseStats = indexShard.getRetentionLeaseStats();
                        } catch (final AlreadyClosedException e) {
                            // shard is closed - no stats is fine
                            commitStats = null;
                            seqNoStats = null;
                            retentionLeaseStats = null;
                        }
                        shardsStats.add(
                            new ShardStats(
                                indexShard.routingEntry(),
                                indexShard.shardPath(),
                                new CommonStats(indicesService.getIndicesQueryCache(), indexShard, commonStatsFlags),
                                commitStats,
                                seqNoStats,
                                retentionLeaseStats
                            )
                        );
                    }
                }
            }
        }

        ClusterHealthStatus clusterStatus = null;
        if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
            clusterStatus = new ClusterStateHealth(clusterService.state(), ClusterHealthRequest.Level.CLUSTER).getStatus();
        }

        return new ClusterStatsNodeResponse(
            nodeInfo.getNode(),
            clusterStatus,
            nodeInfo,
            nodeStats,
            shardsStats.toArray(new ShardStats[0]),
            nodeRequest.request.useAggregatedNodeLevelResponses()
        );
    }

    /**
     * A metric is required when: all cluster stats are required (OR) if the metric is requested
     * @param metric
     * @param clusterStatsRequest
     * @return
     */
    private boolean isMetricRequired(Metric metric, ClusterStatsRequest clusterStatsRequest) {
        return clusterStatsRequest.computeAllMetrics() || clusterStatsRequest.requestedMetrics().contains(metric);
    }

    private static CommonStatsFlags getCommonStatsFlags(ClusterStatsNodeRequest nodeRequest) {
        Set<CommonStatsFlags.Flag> requestedCommonStatsFlags = new HashSet<>();
        if (nodeRequest.request.computeAllMetrics()) {
            requestedCommonStatsFlags.addAll(SHARDS_STATS_FLAG_MAP_TO_INDEX_METRIC.keySet());
        } else {
            for (Map.Entry<CommonStatsFlags.Flag, ClusterStatsRequest.IndexMetric> entry : SHARDS_STATS_FLAG_MAP_TO_INDEX_METRIC
                .entrySet()) {
                if (nodeRequest.request.indicesMetrics().contains(entry.getValue())) {
                    requestedCommonStatsFlags.add(entry.getKey());
                }
            }
        }
        return new CommonStatsFlags(requestedCommonStatsFlags.toArray(new CommonStatsFlags.Flag[0]));
    }

    /**
     * Inner Cluster Stats Node Request
     *
     * @opensearch.internal
     */
    public static class ClusterStatsNodeRequest extends TransportRequest {

        protected ClusterStatsRequest request;

        public ClusterStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new ClusterStatsRequest(in);
        }

        ClusterStatsNodeRequest(ClusterStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
