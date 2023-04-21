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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.store.StoreStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * InternalClusterInfoService provides the ClusterInfoService interface,
 * routinely updated on a timer. The timer can be dynamically changed by
 * setting the <code>cluster.info.update.interval</code> setting (defaulting
 * to 30 seconds). The InternalClusterInfoService only runs on the cluster-manager node.
 * Listens for changes in the number of data nodes and immediately submits a
 * ClusterInfoUpdateJob if a node has been added.
 *
 * Every time the timer runs, gathers information about the disk usage and
 * shard sizes across the cluster.
 *
 * @opensearch.internal
 */
public class InternalClusterInfoService implements ClusterInfoService, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(InternalClusterInfoService.class);

    private static final String REFRESH_EXECUTOR = ThreadPool.Names.MANAGEMENT;

    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.info.update.interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(10),
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<TimeValue> INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "cluster.info.update.timeout",
        TimeValue.timeValueSeconds(15),
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile TimeValue updateFrequency;

    private volatile Map<String, DiskUsage> leastAvailableSpaceUsages;
    private volatile Map<String, DiskUsage> mostAvailableSpaceUsages;
    private volatile IndicesStatsSummary indicesStatsSummary;
    // null if this node is not currently the cluster-manager
    private final AtomicReference<RefreshAndRescheduleRunnable> refreshAndRescheduleRunnable = new AtomicReference<>();
    private volatile boolean enabled;
    private volatile TimeValue fetchTimeout;
    private final ThreadPool threadPool;
    private final Client client;
    private final List<Consumer<ClusterInfo>> listeners = new CopyOnWriteArrayList<>();

    public InternalClusterInfoService(Settings settings, ClusterService clusterService, ThreadPool threadPool, Client client) {
        this.leastAvailableSpaceUsages = Map.of();
        this.mostAvailableSpaceUsages = Map.of();
        this.indicesStatsSummary = IndicesStatsSummary.EMPTY;
        this.threadPool = threadPool;
        this.client = client;
        this.updateFrequency = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings);
        this.fetchTimeout = INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.get(settings);
        this.enabled = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING, this::setFetchTimeout);
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING, this::setUpdateFrequency);
        clusterSettings.addSettingsUpdateConsumer(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
            this::setEnabled
        );
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setFetchTimeout(TimeValue fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    void setUpdateFrequency(TimeValue updateFrequency) {
        this.updateFrequency = updateFrequency;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeClusterManager() && refreshAndRescheduleRunnable.get() == null) {
            logger.trace("elected as cluster-manager, scheduling cluster info update tasks");
            executeRefresh(event.state(), "became cluster-manager");

            final RefreshAndRescheduleRunnable newRunnable = new RefreshAndRescheduleRunnable();
            refreshAndRescheduleRunnable.set(newRunnable);
            threadPool.scheduleUnlessShuttingDown(updateFrequency, REFRESH_EXECUTOR, newRunnable);
        } else if (event.localNodeClusterManager() == false) {
            refreshAndRescheduleRunnable.set(null);
            return;
        }

        if (enabled == false) {
            return;
        }

        // Refresh if a data node was added
        for (DiscoveryNode addedNode : event.nodesDelta().addedNodes()) {
            if (addedNode.isDataNode()) {
                executeRefresh(event.state(), "data node added");
                break;
            }
        }

        // Clean up info for any removed nodes
        for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
            if (removedNode.isDataNode()) {
                logger.trace("Removing node from cluster info: {}", removedNode.getId());
                if (leastAvailableSpaceUsages.containsKey(removedNode.getId())) {
                    Map<String, DiskUsage> newMaxUsages = new HashMap<>(leastAvailableSpaceUsages);
                    newMaxUsages.remove(removedNode.getId());
                    leastAvailableSpaceUsages = Collections.unmodifiableMap(newMaxUsages);
                }
                if (mostAvailableSpaceUsages.containsKey(removedNode.getId())) {
                    Map<String, DiskUsage> newMinUsages = new HashMap<>(mostAvailableSpaceUsages);
                    newMinUsages.remove(removedNode.getId());
                    mostAvailableSpaceUsages = Collections.unmodifiableMap(newMinUsages);
                }
            }
        }
    }

    private void executeRefresh(ClusterState clusterState, String reason) {
        if (clusterState.nodes().getDataNodes().size() > 1) {
            logger.trace("refreshing cluster info in background [{}]", reason);
            threadPool.executor(REFRESH_EXECUTOR).execute(new RefreshRunnable(reason));
        }
    }

    @Override
    public ClusterInfo getClusterInfo() {
        final IndicesStatsSummary indicesStatsSummary = this.indicesStatsSummary; // single volatile read
        return new ClusterInfo(
            leastAvailableSpaceUsages,
            mostAvailableSpaceUsages,
            indicesStatsSummary.shardSizes,
            indicesStatsSummary.shardRoutingToDataPath,
            indicesStatsSummary.reservedSpace
        );
    }

    /**
     * Retrieve the latest nodes stats, calling the listener when complete
     * @return a latch that can be used to wait for the nodes stats to complete if desired
     */
    protected CountDownLatch updateNodeStats(final ActionListener<NodesStatsResponse> listener) {
        final CountDownLatch latch = new CountDownLatch(1);
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
        nodesStatsRequest.clear();
        nodesStatsRequest.addMetric(NodesStatsRequest.Metric.FS.metricName());
        nodesStatsRequest.timeout(fetchTimeout);
        client.admin().cluster().nodesStats(nodesStatsRequest, new LatchedActionListener<>(listener, latch));
        return latch;
    }

    /**
     * Retrieve the latest indices stats, calling the listener when complete
     * @return a latch that can be used to wait for the indices stats to complete if desired
     */
    protected CountDownLatch updateIndicesStats(final ActionListener<IndicesStatsResponse> listener) {
        final CountDownLatch latch = new CountDownLatch(1);
        final IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.clear();
        indicesStatsRequest.store(true);
        indicesStatsRequest.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED_HIDDEN);

        client.admin().indices().stats(indicesStatsRequest, new LatchedActionListener<>(listener, latch));
        return latch;
    }

    // allow tests to adjust the node stats on receipt
    List<NodeStats> adjustNodesStats(List<NodeStats> nodeStats) {
        return nodeStats;
    }

    /**
     * Refreshes the ClusterInfo in a blocking fashion
     */
    public final ClusterInfo refresh() {
        logger.trace("refreshing cluster info");
        final CountDownLatch nodeLatch = updateNodeStats(new ActionListener<NodesStatsResponse>() {
            @Override
            public void onResponse(NodesStatsResponse nodesStatsResponse) {
                final Map<String, DiskUsage> leastAvailableUsagesBuilder = new HashMap<>();
                final Map<String, DiskUsage> mostAvailableUsagesBuilder = new HashMap<>();
                fillDiskUsagePerNode(
                    logger,
                    adjustNodesStats(nodesStatsResponse.getNodes()),
                    leastAvailableUsagesBuilder,
                    mostAvailableUsagesBuilder
                );
                leastAvailableSpaceUsages = Collections.unmodifiableMap(leastAvailableUsagesBuilder);
                mostAvailableSpaceUsages = Collections.unmodifiableMap(mostAvailableUsagesBuilder);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ReceiveTimeoutTransportException) {
                    logger.error("NodeStatsAction timed out for ClusterInfoUpdateJob", e);
                } else {
                    if (e instanceof ClusterBlockException) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                        }
                    } else {
                        logger.warn("Failed to execute NodeStatsAction for ClusterInfoUpdateJob", e);
                    }
                    // we empty the usages list, to be safe - we don't know what's going on.
                    leastAvailableSpaceUsages = Map.of();
                    mostAvailableSpaceUsages = Map.of();
                }
            }
        });

        final CountDownLatch indicesLatch = updateIndicesStats(new ActionListener<>() {
            @Override
            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                final ShardStats[] stats = indicesStatsResponse.getShards();
                final Map<String, Long> shardSizeByIdentifierBuilder = new HashMap<>();
                final Map<ShardRouting, String> dataPathByShardRoutingBuilder = new HashMap<>();
                final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceBuilders = new HashMap<>();
                buildShardLevelInfo(logger, stats, shardSizeByIdentifierBuilder, dataPathByShardRoutingBuilder, reservedSpaceBuilders);

                final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> rsrvdSpace = new HashMap<>();
                reservedSpaceBuilders.forEach((nodeAndPath, builder) -> rsrvdSpace.put(nodeAndPath, builder.build()));

                indicesStatsSummary = new IndicesStatsSummary(shardSizeByIdentifierBuilder, dataPathByShardRoutingBuilder, rsrvdSpace);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ReceiveTimeoutTransportException) {
                    logger.error("IndicesStatsAction timed out for ClusterInfoUpdateJob", e);
                } else {
                    if (e instanceof ClusterBlockException) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                        }
                    } else {
                        logger.warn("Failed to execute IndicesStatsAction for ClusterInfoUpdateJob", e);
                    }
                    // we empty the usages list, to be safe - we don't know what's going on.
                    indicesStatsSummary = IndicesStatsSummary.EMPTY;
                }
            }
        });

        try {
            if (nodeLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                logger.warn("Failed to update node information for ClusterInfoUpdateJob within {} timeout", fetchTimeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
        }

        try {
            if (indicesLatch.await(fetchTimeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                logger.warn("Failed to update shard information for ClusterInfoUpdateJob within {} timeout", fetchTimeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
        }
        ClusterInfo clusterInfo = getClusterInfo();
        boolean anyListeners = false;
        for (final Consumer<ClusterInfo> listener : listeners) {
            anyListeners = true;
            try {
                logger.trace("notifying [{}] of new cluster info", listener);
                listener.accept(clusterInfo);
            } catch (Exception e) {
                logger.info(new ParameterizedMessage("failed to notify [{}] of new cluster info", listener), e);
            }
        }
        assert anyListeners : "expected to notify at least one listener";
        return clusterInfo;
    }

    @Override
    public void addListener(Consumer<ClusterInfo> clusterInfoConsumer) {
        listeners.add(clusterInfoConsumer);
    }

    static void buildShardLevelInfo(
        Logger logger,
        ShardStats[] stats,
        final Map<String, Long> shardSizes,
        final Map<ShardRouting, String> newShardRoutingToDataPath,
        final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace.Builder> reservedSpaceByShard
    ) {
        for (ShardStats s : stats) {
            final ShardRouting shardRouting = s.getShardRouting();
            newShardRoutingToDataPath.put(shardRouting, s.getDataPath());

            final StoreStats storeStats = s.getStats().getStore();
            if (storeStats == null) {
                continue;
            }
            final long size = storeStats.sizeInBytes();
            final long reserved = storeStats.getReservedSize().getBytes();

            final String shardIdentifier = ClusterInfo.shardIdentifierFromRouting(shardRouting);
            logger.trace("shard: {} size: {} reserved: {}", shardIdentifier, size, reserved);
            shardSizes.put(shardIdentifier, size);

            if (reserved != StoreStats.UNKNOWN_RESERVED_BYTES) {
                final ClusterInfo.ReservedSpace.Builder reservedSpaceBuilder = reservedSpaceByShard.computeIfAbsent(
                    new ClusterInfo.NodeAndPath(shardRouting.currentNodeId(), s.getDataPath()),
                    t -> new ClusterInfo.ReservedSpace.Builder()
                );
                reservedSpaceBuilder.add(shardRouting.shardId(), reserved);
            }
        }
    }

    static void fillDiskUsagePerNode(
        Logger logger,
        List<NodeStats> nodeStatsArray,
        final Map<String, DiskUsage> newLeastAvailableUsages,
        final Map<String, DiskUsage> newMostAvailableUsages
    ) {
        for (NodeStats nodeStats : nodeStatsArray) {
            if (nodeStats.getFs() == null) {
                logger.warn("Unable to retrieve node FS stats for {}", nodeStats.getNode().getName());
            } else {
                FsInfo.Path leastAvailablePath = null;
                FsInfo.Path mostAvailablePath = null;
                for (FsInfo.Path info : nodeStats.getFs()) {
                    if (leastAvailablePath == null) {
                        assert mostAvailablePath == null;
                        mostAvailablePath = leastAvailablePath = info;
                    } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()) {
                        leastAvailablePath = info;
                    } else if (mostAvailablePath.getAvailable().getBytes() < info.getAvailable().getBytes()) {
                        mostAvailablePath = info;
                    }
                }
                String nodeId = nodeStats.getNode().getId();
                String nodeName = nodeStats.getNode().getName();
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "node: [{}], most available: total disk: {},"
                            + " available disk: {} / least available: total disk: {}, available disk: {}",
                        nodeId,
                        mostAvailablePath.getTotal(),
                        mostAvailablePath.getAvailable(),
                        leastAvailablePath.getTotal(),
                        leastAvailablePath.getAvailable()
                    );
                }
                if (leastAvailablePath.getTotal().getBytes() < 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "node: [{}] least available path has less than 0 total bytes of disk [{}], skipping",
                            nodeId,
                            leastAvailablePath.getTotal().getBytes()
                        );
                    }
                } else {
                    newLeastAvailableUsages.put(
                        nodeId,
                        new DiskUsage(
                            nodeId,
                            nodeName,
                            leastAvailablePath.getPath(),
                            leastAvailablePath.getTotal().getBytes(),
                            leastAvailablePath.getAvailable().getBytes()
                        )
                    );
                }
                if (mostAvailablePath.getTotal().getBytes() < 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "node: [{}] most available path has less than 0 total bytes of disk [{}], skipping",
                            nodeId,
                            mostAvailablePath.getTotal().getBytes()
                        );
                    }
                } else {
                    newMostAvailableUsages.put(
                        nodeId,
                        new DiskUsage(
                            nodeId,
                            nodeName,
                            mostAvailablePath.getPath(),
                            mostAvailablePath.getTotal().getBytes(),
                            mostAvailablePath.getAvailable().getBytes()
                        )
                    );
                }

            }
        }
    }

    /**
     * Indices statistics summary.
     *
     * @opensearch.internal
     */
    private static class IndicesStatsSummary {
        static final IndicesStatsSummary EMPTY = new IndicesStatsSummary(Map.of(), Map.of(), Map.of());

        final Map<String, Long> shardSizes;
        final Map<ShardRouting, String> shardRoutingToDataPath;
        final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace;

        IndicesStatsSummary(
            final Map<String, Long> shardSizes,
            final Map<ShardRouting, String> shardRoutingToDataPath,
            final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace
        ) {
            this.shardSizes = shardSizes;
            this.shardRoutingToDataPath = shardRoutingToDataPath;
            this.reservedSpace = reservedSpace;
        }
    }

    /**
     * Runs {@link InternalClusterInfoService#refresh()}, logging failures/rejections appropriately.
     *
     * @opensearch.internal
     */
    private class RefreshRunnable extends AbstractRunnable {
        private final String reason;

        RefreshRunnable(String reason) {
            this.reason = reason;
        }

        @Override
        protected void doRun() {
            if (enabled) {
                logger.trace("refreshing cluster info [{}]", reason);
                refresh();
            } else {
                logger.trace("skipping cluster info refresh [{}] since it is disabled", reason);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(new ParameterizedMessage("refreshing cluster info failed [{}]", reason), e);
        }

        @Override
        public void onRejection(Exception e) {
            final boolean shutDown = e instanceof OpenSearchRejectedExecutionException
                && ((OpenSearchRejectedExecutionException) e).isExecutorShutdown();
            logger.log(shutDown ? Level.DEBUG : Level.WARN, "refreshing cluster info rejected [{}]", reason, e);
        }
    }

    /**
     * Runs {@link InternalClusterInfoService#refresh()}, logging failures/rejections appropriately, and reschedules itself on completion.
     *
     * @opensearch.internal
     */
    private class RefreshAndRescheduleRunnable extends RefreshRunnable {
        RefreshAndRescheduleRunnable() {
            super("scheduled");
        }

        @Override
        protected void doRun() {
            if (this == refreshAndRescheduleRunnable.get()) {
                super.doRun();
            } else {
                logger.trace("cluster-manager changed, scheduled refresh job is stale");
            }
        }

        @Override
        public void onAfter() {
            if (this == refreshAndRescheduleRunnable.get()) {
                logger.trace("scheduling next cluster info refresh in [{}]", updateFrequency);
                threadPool.scheduleUnlessShuttingDown(updateFrequency, REFRESH_EXECUTOR, this);
            }
        }
    }
}
