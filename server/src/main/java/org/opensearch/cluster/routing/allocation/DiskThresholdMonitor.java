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

package org.opensearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.opensearch.common.Priority;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Listens for a node to go over the high watermark and kicks off an empty
 * reroute if it does. Also responsible for logging about nodes that have
 * passed the disk watermarks
 *
 * @opensearch.internal
 */
public class DiskThresholdMonitor {

    private static final Logger logger = LogManager.getLogger(DiskThresholdMonitor.class);
    private final DiskThresholdSettings diskThresholdSettings;
    private final Client client;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;
    private final AtomicLong lastRunTimeMillis = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean checkInProgress = new AtomicBoolean();

    /**
     * The IDs of the nodes that were over the low threshold in the last check (and maybe over another threshold too). Tracked so that we
     * can log when such nodes are no longer over the low threshold.
     */
    private final Set<String> nodesOverLowThreshold = Sets.newConcurrentHashSet();

    /**
     * The IDs of the nodes that were over the high threshold in the last check (and maybe over another threshold too). Tracked so that we
     * can log when such nodes are no longer over the high threshold.
     */
    private final Set<String> nodesOverHighThreshold = Sets.newConcurrentHashSet();

    /**
     * The IDs of the nodes that were over the high threshold in the last check, but which are relocating shards that will bring them
     * under the high threshold again. Tracked so that we can log when such nodes are no longer in this state.
     */
    private final Set<String> nodesOverHighThresholdAndRelocating = Sets.newConcurrentHashSet();

    public DiskThresholdMonitor(
        Settings settings,
        Supplier<ClusterState> clusterStateSupplier,
        ClusterSettings clusterSettings,
        Client client,
        LongSupplier currentTimeMillisSupplier,
        RerouteService rerouteService
    ) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
        this.client = client;
    }

    private void checkFinished() {
        final boolean checkFinished = checkInProgress.compareAndSet(true, false);
        assert checkFinished;
        logger.trace("checkFinished");
    }

    public void onNewInfo(ClusterInfo info) {
        // TODO find a better way to limit concurrent updates (and potential associated reroutes) while allowing tests to ensure that
        // all ClusterInfo updates are processed and never ignored
        if (checkInProgress.compareAndSet(false, true) == false) {
            logger.info("skipping monitor as a check is already in progress");
            return;
        }

        final Map<String, DiskUsage> usages = info.getNodeLeastAvailableDiskUsages();
        if (usages == null) {
            logger.trace("skipping monitor as no disk usage information is available");
            checkFinished();
            return;
        }

        logger.trace("processing new cluster info");

        boolean reroute = false;
        String explanation = "";
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();

        // Clean up nodes that have been removed from the cluster
        final Set<String> nodes = usages.keySet();
        cleanUpRemovedNodes(nodes, nodesOverLowThreshold);
        cleanUpRemovedNodes(nodes, nodesOverHighThreshold);
        cleanUpRemovedNodes(nodes, nodesOverHighThresholdAndRelocating);

        final ClusterState state = clusterStateSupplier.get();
        final Set<String> indicesToMarkReadOnly = new HashSet<>();
        RoutingNodes routingNodes = state.getRoutingNodes();
        Set<String> indicesNotToAutoRelease = new HashSet<>();
        markNodesMissingUsageIneligibleForRelease(routingNodes, usages, indicesNotToAutoRelease);

        final List<DiskUsage> usagesOverHighThreshold = new ArrayList<>();

        for (final Map.Entry<String, DiskUsage> entry : usages.entrySet()) {
            final String node = entry.getKey();
            final DiskUsage usage = entry.getValue();
            final RoutingNode routingNode = routingNodes.node(node);

            if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdFloodStage().getBytes()
                || usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdFloodStage()) {

                nodesOverLowThreshold.add(node);
                nodesOverHighThreshold.add(node);
                nodesOverHighThresholdAndRelocating.remove(node);

                if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                    for (ShardRouting routing : routingNode) {
                        String indexName = routing.index().getName();
                        indicesToMarkReadOnly.add(indexName);
                        indicesNotToAutoRelease.add(indexName);
                    }
                }

                logger.warn(
                    "flood stage disk watermark [{}] exceeded on {}, all indices on this node will be marked read-only",
                    diskThresholdSettings.describeFloodStageThreshold(),
                    usage
                );

                continue;
            }

            if (usage.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()
                || usage.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh()) {

                if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                    for (ShardRouting routing : routingNode) {
                        String indexName = routing.index().getName();
                        indicesNotToAutoRelease.add(indexName);
                    }
                }
            }

            final long reservedSpace = info.getReservedSpace(usage.getNodeId(), usage.getPath()).getTotal();
            final DiskUsage usageWithReservedSpace = new DiskUsage(
                usage.getNodeId(),
                usage.getNodeName(),
                usage.getPath(),
                usage.getTotalBytes(),
                Math.max(0L, usage.getFreeBytes() - reservedSpace)
            );

            if (usageWithReservedSpace.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()
                || usageWithReservedSpace.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh()) {

                nodesOverLowThreshold.add(node);
                nodesOverHighThreshold.add(node);

                if (lastRunTimeMillis.get() <= currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                    reroute = true;
                    explanation = "high disk watermark exceeded on one or more nodes";
                    usagesOverHighThreshold.add(usage);
                    // will log about this node when the reroute completes
                } else {
                    logger.debug(
                        "high disk watermark exceeded on {} but an automatic reroute has occurred " + "in the last [{}], skipping reroute",
                        node,
                        diskThresholdSettings.getRerouteInterval()
                    );
                }

            } else if (usageWithReservedSpace.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()
                || usageWithReservedSpace.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdLow()) {

                    nodesOverHighThresholdAndRelocating.remove(node);

                    final boolean wasUnderLowThreshold = nodesOverLowThreshold.add(node);
                    final boolean wasOverHighThreshold = nodesOverHighThreshold.remove(node);
                    assert (wasUnderLowThreshold && wasOverHighThreshold) == false;

                    if (wasUnderLowThreshold) {
                        logger.info(
                            "low disk watermark [{}] exceeded on {}, replicas will not be assigned to this node",
                            diskThresholdSettings.describeLowThreshold(),
                            usage
                        );
                    } else if (wasOverHighThreshold) {
                        logger.info(
                            "high disk watermark [{}] no longer exceeded on {}, but low disk watermark [{}] is still exceeded",
                            diskThresholdSettings.describeHighThreshold(),
                            usage,
                            diskThresholdSettings.describeLowThreshold()
                        );
                    }

                } else {

                    nodesOverHighThresholdAndRelocating.remove(node);

                    if (nodesOverLowThreshold.contains(node)) {
                        // The node has previously been over the low watermark, but is no longer, so it may be possible to allocate more
                        // shards
                        // if we reroute now.
                        if (lastRunTimeMillis.get() <= currentTimeMillis - diskThresholdSettings.getRerouteInterval().millis()) {
                            reroute = true;
                            explanation = "one or more nodes has gone under the high or low watermark";
                            nodesOverLowThreshold.remove(node);
                            nodesOverHighThreshold.remove(node);

                            logger.info(
                                "low disk watermark [{}] no longer exceeded on {}",
                                diskThresholdSettings.describeLowThreshold(),
                                usage
                            );

                        } else {
                            logger.debug(
                                "{} has gone below a disk threshold, but an automatic reroute has occurred "
                                    + "in the last [{}], skipping reroute",
                                node,
                                diskThresholdSettings.getRerouteInterval()
                            );
                        }
                    }

                }
        }

        final ActionListener<Void> listener = new GroupedActionListener<>(ActionListener.wrap(this::checkFinished), 4);

        if (reroute) {
            logger.debug("rerouting shards: [{}]", explanation);
            rerouteService.reroute("disk threshold monitor", Priority.HIGH, ActionListener.wrap(reroutedClusterState -> {

                for (DiskUsage diskUsage : usagesOverHighThreshold) {
                    final RoutingNode routingNode = reroutedClusterState.getRoutingNodes().node(diskUsage.getNodeId());
                    final DiskUsage usageIncludingRelocations;
                    final long relocatingShardsSize;
                    if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
                        relocatingShardsSize = sizeOfRelocatingShards(routingNode, diskUsage, info, reroutedClusterState);
                        usageIncludingRelocations = new DiskUsage(
                            diskUsage.getNodeId(),
                            diskUsage.getNodeName(),
                            diskUsage.getPath(),
                            diskUsage.getTotalBytes(),
                            diskUsage.getFreeBytes() - relocatingShardsSize
                        );
                    } else {
                        usageIncludingRelocations = diskUsage;
                        relocatingShardsSize = 0L;
                    }

                    if (usageIncludingRelocations.getFreeBytes() < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()
                        || usageIncludingRelocations.getFreeDiskAsPercentage() < diskThresholdSettings.getFreeDiskThresholdHigh()) {

                        nodesOverHighThresholdAndRelocating.remove(diskUsage.getNodeId());
                        logger.warn(
                            "high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; "
                                + "currently relocating away shards totalling [{}] bytes; the node is expected to continue to exceed "
                                + "the high disk watermark when these relocations are complete",
                            diskThresholdSettings.describeHighThreshold(),
                            diskUsage,
                            -relocatingShardsSize
                        );
                    } else if (nodesOverHighThresholdAndRelocating.add(diskUsage.getNodeId())) {
                        logger.info(
                            "high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; "
                                + "currently relocating away shards totalling [{}] bytes; the node is expected to be below the high "
                                + "disk watermark when these relocations are complete",
                            diskThresholdSettings.describeHighThreshold(),
                            diskUsage,
                            -relocatingShardsSize
                        );
                    } else {
                        logger.debug(
                            "high disk watermark [{}] exceeded on {}, shards will be relocated away from this node; "
                                + "currently relocating away shards totalling [{}] bytes",
                            diskThresholdSettings.describeHighThreshold(),
                            diskUsage,
                            -relocatingShardsSize
                        );
                    }
                }

                setLastRunTimeMillis();
                listener.onResponse(null);
            }, e -> {
                logger.debug("reroute failed", e);
                setLastRunTimeMillis();
                listener.onFailure(e);
            }));
        } else {
            logger.trace("no reroute required");
            listener.onResponse(null);
        }
        final Set<String> indicesToAutoRelease = StreamSupport.stream(
            Spliterators.spliterator(state.routingTable().indicesRouting().entrySet(), 0),
            false
        )
            .map(c -> c.getKey())
            .filter(index -> indicesNotToAutoRelease.contains(index) == false)
            .filter(index -> state.getBlocks().hasIndexBlock(index, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .collect(Collectors.toSet());

        if (indicesToAutoRelease.isEmpty() == false) {
            updateIndicesReadOnly(indicesToAutoRelease, listener, false);
        } else {
            logger.trace("no auto-release required");
            listener.onResponse(null);
        }

        indicesToMarkReadOnly.removeIf(index -> state.getBlocks().indexBlocked(ClusterBlockLevel.WRITE, index));
        logger.trace("marking indices as read-only: [{}]", indicesToMarkReadOnly);
        if (indicesToMarkReadOnly.isEmpty() == false) {
            updateIndicesReadOnly(indicesToMarkReadOnly, listener, true);
        } else {
            listener.onResponse(null);
        }

        // If all the nodes are breaching high disk watermark, we apply index create block to avoid red clusters.
        if ((state.getBlocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()) == false)
            && nodes.size() > 0
            && nodesOverHighThreshold.size() == nodes.size()) {
            setIndexCreateBlock(listener, true);
        } else if (state.getBlocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id())
            && diskThresholdSettings.isCreateIndexBlockAutoReleaseEnabled()) {
                setIndexCreateBlock(listener, false);
            } else {
                listener.onResponse(null);
            }
    }

    // exposed for tests to override
    long sizeOfRelocatingShards(RoutingNode routingNode, DiskUsage diskUsage, ClusterInfo info, ClusterState reroutedClusterState) {
        return DiskThresholdDecider.sizeOfRelocatingShards(
            routingNode,
            true,
            diskUsage.getPath(),
            info,
            reroutedClusterState.metadata(),
            reroutedClusterState.routingTable()
        );
    }

    private void markNodesMissingUsageIneligibleForRelease(
        RoutingNodes routingNodes,
        Map<String, DiskUsage> usages,
        Set<String> indicesToMarkIneligibleForAutoRelease
    ) {
        for (RoutingNode routingNode : routingNodes) {
            if (usages.containsKey(routingNode.nodeId()) == false) {
                for (ShardRouting routing : routingNode) {
                    String indexName = routing.index().getName();
                    indicesToMarkIneligibleForAutoRelease.add(indexName);
                }
            }
        }
    }

    private void setLastRunTimeMillis() {
        lastRunTimeMillis.getAndUpdate(l -> Math.max(l, currentTimeMillisSupplier.getAsLong()));
    }

    protected void setIndexCreateBlock(final ActionListener<Void> listener, boolean indexCreateBlock) {
        final ActionListener<Void> wrappedListener = ActionListener.wrap(r -> {
            setLastRunTimeMillis();
            listener.onResponse(r);
        }, e -> {
            logger.debug("setting index create block failed", e);
            setLastRunTimeMillis();
            listener.onFailure(e);
        });

        final Settings indexCreateBlockSetting = indexCreateBlock
            ? Settings.builder().put(Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING.getKey(), Boolean.TRUE.toString()).build()
            : Settings.builder().putNull(Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING.getKey()).build();

        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(indexCreateBlockSetting)
            .execute(ActionListener.map(wrappedListener, r -> null));
    }

    protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
        // set read-only block but don't block on the response
        ActionListener<Void> wrappedListener = ActionListener.wrap(r -> {
            setLastRunTimeMillis();
            listener.onResponse(r);
        }, e -> {
            logger.debug(new ParameterizedMessage("setting indices [{}] read-only failed", readOnly), e);
            setLastRunTimeMillis();
            listener.onFailure(e);
        });
        Settings readOnlySettings = readOnly
            ? Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, Boolean.TRUE.toString()).build()
            : Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build();
        client.admin()
            .indices()
            .prepareUpdateSettings(indicesToUpdate.toArray(Strings.EMPTY_ARRAY))
            .setSettings(readOnlySettings)
            .execute(ActionListener.map(wrappedListener, r -> null));
    }

    private static void cleanUpRemovedNodes(Set<String> nodesToKeep, Set<String> nodesToCleanUp) {
        for (String node : nodesToCleanUp) {
            if (nodesToKeep.contains(node) == false) {
                nodesToCleanUp.remove(node);
            }
        }
    }
}
