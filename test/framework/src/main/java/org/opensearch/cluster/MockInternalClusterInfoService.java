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

package org.opensearch.cluster;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MockInternalClusterInfoService extends InternalClusterInfoService {

    /** This is a marker plugin used to trigger MockNode to use this mock info service. */
    public static class TestPlugin extends Plugin {}

    @Nullable // if no fakery should take place
    private volatile Function<ShardRouting, Long> shardSizeFunction;

    @Nullable // if no fakery should take place
    private volatile BiFunction<DiscoveryNode, FsInfo.Path, FsInfo.Path> diskUsageFunction;

    public MockInternalClusterInfoService(Settings settings, ClusterService clusterService, ThreadPool threadPool, NodeClient client) {
        super(settings, clusterService, threadPool, client);
    }

    public void setDiskUsageFunctionAndRefresh(BiFunction<DiscoveryNode, FsInfo.Path, FsInfo.Path> diskUsageFunction) {
        this.diskUsageFunction = diskUsageFunction;
        refresh();
    }

    public void setShardSizeFunctionAndRefresh(Function<ShardRouting, Long> shardSizeFunction) {
        this.shardSizeFunction = shardSizeFunction;
        refresh();
    }

    @Override
    public ClusterInfo getClusterInfo() {
        final ClusterInfo clusterInfo = super.getClusterInfo();
        return new SizeFakingClusterInfo(clusterInfo);
    }

    @Override
    List<NodeStats> adjustNodesStats(List<NodeStats> nodesStats) {
        final BiFunction<DiscoveryNode, FsInfo.Path, FsInfo.Path> diskUsageFunction = this.diskUsageFunction;
        if (diskUsageFunction == null) {
            return nodesStats;
        }

        return nodesStats.stream().map(nodeStats -> {
            final DiscoveryNode discoveryNode = nodeStats.getNode();
            final FsInfo oldFsInfo = nodeStats.getFs();
            return new NodeStats(
                discoveryNode,
                nodeStats.getTimestamp(),
                nodeStats.getIndices(),
                nodeStats.getOs(),
                nodeStats.getProcess(),
                nodeStats.getJvm(),
                nodeStats.getThreadPool(),
                new FsInfo(
                    oldFsInfo.getTimestamp(),
                    oldFsInfo.getIoStats(),
                    StreamSupport.stream(oldFsInfo.spliterator(), false)
                        .map(fsInfoPath -> diskUsageFunction.apply(discoveryNode, fsInfoPath))
                        .toArray(FsInfo.Path[]::new)
                ),
                nodeStats.getTransport(),
                nodeStats.getHttp(),
                nodeStats.getBreaker(),
                nodeStats.getScriptStats(),
                nodeStats.getDiscoveryStats(),
                nodeStats.getIngestStats(),
                nodeStats.getAdaptiveSelectionStats(),
                nodeStats.getScriptCacheStats(),
                nodeStats.getIndexingPressureStats(),
                nodeStats.getShardIndexingPressureStats(),
                nodeStats.getSearchBackpressureStats(),
                nodeStats.getClusterManagerThrottlingStats(),
                nodeStats.getWeightedRoutingStats(),
                nodeStats.getFileCacheStats()
            );
        }).collect(Collectors.toList());
    }

    class SizeFakingClusterInfo extends ClusterInfo {
        SizeFakingClusterInfo(ClusterInfo delegate) {
            super(
                delegate.getNodeLeastAvailableDiskUsages(),
                delegate.getNodeMostAvailableDiskUsages(),
                delegate.shardSizes,
                delegate.routingToDataPath,
                delegate.reservedSpace
            );
        }

        @Override
        public Long getShardSize(ShardRouting shardRouting) {
            final Function<ShardRouting, Long> shardSizeFunction = MockInternalClusterInfoService.this.shardSizeFunction;
            if (shardSizeFunction == null) {
                return super.getShardSize(shardRouting);
            }

            return shardSizeFunction.apply(shardRouting);
        }
    }

    @Override
    public void setUpdateFrequency(TimeValue updateFrequency) {
        super.setUpdateFrequency(updateFrequency);
    }
}
