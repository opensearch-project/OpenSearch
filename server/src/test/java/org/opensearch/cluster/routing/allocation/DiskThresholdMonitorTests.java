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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.junit.annotations.TestLogging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class DiskThresholdMonitorTests extends OpenSearchAllocationTestCase {

    public void testMarkFloodStageIndicesReadOnly() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "hot_node_2"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_1")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "hot_node_1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_2")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "hot_node_1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test_1"))
            .addAsNew(metadata.index("test_2"))
            .build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(newNode("hot_node_1")).add(newNode("hot_node_2")))
                .build(),
            allocation
        );
        AtomicBoolean reroute = new AtomicBoolean(false);
        AtomicReference<Set<String>> indices = new AtomicReference<>();
        AtomicLong currentTime = new AtomicLong();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 5.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToBlockRead, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };

        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, 4));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, 30));
        monitor.onNewInfo(clusterInfo(builder));
        assertFalse(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indices.get());

        indices.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, 4));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, 5));
        currentTime.addAndGet(randomLongBetween(60001, 120000));
        monitor.onNewInfo(clusterInfo(builder));
        assertTrue(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indices.get());
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().index("test_2").getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
            )
            .build();

        // now we mark one index as read-only and assert that we don't mark it as such again
        final ClusterState anotherFinalClusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(clusterState.metadata().index("test"), false)
                    .put(clusterState.metadata().index("test_1"), false)
                    .put(indexMetadata, true)
                    .build()
            )
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata).build())
            .build();
        assertTrue(anotherFinalClusterState.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));

        monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> anotherFinalClusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 5.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToBlockRead, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };

        indices.set(null);
        reroute.set(false);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, 4));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, 5));
        monitor.onNewInfo(clusterInfo(builder));
        assertTrue(reroute.get());
        assertEquals(Collections.singleton("test_1"), indices.get());
    }

    public void testDoesNotSubmitRerouteTaskTooFrequently() {
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNode("hot_node_1")).add(newNode("hot_node_2")))
            .build();
        AtomicLong currentTime = new AtomicLong();
        AtomicReference<ActionListener<ClusterState>> listenerReference = new AtomicReference<>();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                assertTrue(listenerReference.compareAndSet(null, listener));
            },
            () -> 5.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                throw new AssertionError("unexpected");
            }
        };

        final Map<String, DiskUsage> allDisksOk = new HashMap<>();
        allDisksOk.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, 50));
        allDisksOk.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, 50));

        final Map<String, DiskUsage> oneDiskAboveWatermark = new HashMap<>();
        oneDiskAboveWatermark.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(5, 9)));
        oneDiskAboveWatermark.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, 50));

        // should not reroute when all disks are ok
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // should reroute when one disk goes over the watermark
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(oneDiskAboveWatermark));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(clusterState);

        if (randomBoolean()) {
            // should not re-route again within the reroute interval
            currentTime.addAndGet(
                randomLongBetween(0, DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis())
            );
            monitor.onNewInfo(clusterInfo(allDisksOk));
            assertNull(listenerReference.get());
        }

        // should reroute again when one disk is still over the watermark
        currentTime.addAndGet(
            randomLongBetween(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis() + 1,
                120000
            )
        );
        monitor.onNewInfo(clusterInfo(oneDiskAboveWatermark));
        assertNotNull(listenerReference.get());
        final ActionListener<ClusterState> rerouteListener1 = listenerReference.getAndSet(null);

        // should not re-route again before reroute has completed
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // complete reroute
        rerouteListener1.onResponse(clusterState);

        if (randomBoolean()) {
            // should not re-route again within the reroute interval
            currentTime.addAndGet(
                randomLongBetween(0, DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis())
            );
            monitor.onNewInfo(clusterInfo(allDisksOk));
            assertNull(listenerReference.get());
        }

        // should reroute again after the reroute interval
        currentTime.addAndGet(
            randomLongBetween(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis() + 1,
                120000
            )
        );
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(null);

        // should not reroute again when it is not required
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // should reroute again when one disk has reserved space that pushes it over the high watermark
        final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = new HashMap<>(1);
        reservedSpaces.put(
            new ClusterInfo.NodeAndPath("hot_node_1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("baz", "quux", 0), between(41, 100)).build()
        );

        currentTime.addAndGet(
            randomLongBetween(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis() + 1,
                120000
            )
        );
        monitor.onNewInfo(clusterInfo(allDisksOk, reservedSpaces, Map.of()));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(null);
    }

    public void testAutoReleaseIndices() {
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToReleaseReadOnlyBlock = new AtomicReference<>();
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test_1").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .put(IndexMetadata.builder("test_2").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test_1")).addAsNew(metadata.index("test_2")).build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(newNode("hot_node_1")).add(newNode("hot_node_2")))
                .build(),
            allocation
        );
        assertThat(clusterState.getRoutingTable().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(8));

        final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = new HashMap<>();
        final int reservedSpaceNode1 = between(0, 10);
        reservedSpaces.put(
            new ClusterInfo.NodeAndPath("hot_node_1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode1).build()
        );
        final int reservedSpaceNode2 = between(0, 10);
        reservedSpaces.put(
            new ClusterInfo.NodeAndPath("hot_node_2", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode2).build()
        );

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(clusterState);
            },
            () -> 5.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToReleaseReadOnlyBlock.compareAndSet(null, indicesToUpdate));
                }
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(0, 4)));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces, Map.of()));
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertNull(indicesToReleaseReadOnlyBlock.get());

        // Reserved space is ignored when applying block
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(5, 90)));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, between(5, 90)));
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces, Map.of()));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToReleaseReadOnlyBlock.get());

        // Change cluster state so that "test_2" index is blocked (read only)
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().index("test_2").getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
            )
            .build();

        ClusterState clusterStateWithBlocks = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata).build())
            .build();

        assertTrue(clusterStateWithBlocks.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));
        monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterStateWithBlocks,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(clusterStateWithBlocks);
            },
            () -> 5.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToReleaseReadOnlyBlock.compareAndSet(null, indicesToUpdate));
                }
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };
        // When free disk on any of node1 or node2 goes below 5% flood watermark, then apply index block on indices not having the block
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(0, 100)));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces, Map.of()));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToReleaseReadOnlyBlock.get());

        // When free disk on node1 and node2 goes above 10% high watermark then release index block, ignoring reserved space
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(10, 100)));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, between(10, 100)));
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces, Map.of()));
        assertNull(indicesToMarkReadOnly.get());
        assertThat(indicesToReleaseReadOnlyBlock.get(), contains("test_2"));

        // When no usage information is present for node2, we don't release the block
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(clusterInfo(builder));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToReleaseReadOnlyBlock.get());

        // When disk usage on one node is between the high and flood-stage watermarks, nothing changes
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(5, 9)));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, between(5, 100)));
        if (randomBoolean()) {
            builder.put("node3", new DiskUsage("node3", "node3", "/foo/bar", 100, between(0, 100)));
        }
        monitor.onNewInfo(clusterInfo(builder));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToReleaseReadOnlyBlock.get());

        // When disk usage on one node is missing and the other is below the high watermark, nothing changes
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(5, 100)));
        if (randomBoolean()) {
            builder.put("node3", new DiskUsage("node3", "node3", "/foo/bar", 100, between(0, 100)));
        }
        monitor.onNewInfo(clusterInfo(builder));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToReleaseReadOnlyBlock.get());

        // When disk usage on one node is missing and the other is above the flood-stage watermark, affected indices are blocked
        indicesToMarkReadOnly.set(null);
        indicesToReleaseReadOnlyBlock.set(null);
        builder = new HashMap<>();
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(0, 4)));
        if (randomBoolean()) {
            builder.put("node3", new DiskUsage("node3", "node3", "/foo/bar", 100, between(0, 100)));
        }
        monitor.onNewInfo(clusterInfo(builder));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToReleaseReadOnlyBlock.get());
    }

    @TestLogging(value = "org.opensearch.cluster.routing.allocation.DiskThresholdMonitor:INFO", reason = "testing INFO/WARN logging")
    public void testDiskMonitorLogging() throws IllegalAccessException {
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNode("hot_node_1")))
            .build();
        final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(clusterState);
        final AtomicBoolean advanceTime = new AtomicBoolean(randomBoolean());

        final LongSupplier timeSupplier = new LongSupplier() {
            long time;

            @Override
            public long getAsLong() {
                if (advanceTime.get()) {
                    time += DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).getMillis() + 1;
                }
                logger.info("time: [{}]", time);
                return time;
            }
        };

        final AtomicLong relocatingShardSizeRef = new AtomicLong();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            clusterStateRef::get,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            timeSupplier,
            (reason, priority, listener) -> listener.onResponse(clusterStateRef.get()),
            () -> 5.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                listener.onResponse(null);
            }

            @Override
            long sizeOfRelocatingShards(RoutingNode routingNode, DiskUsage diskUsage, ClusterInfo info, ClusterState reroutedClusterState) {
                return relocatingShardSizeRef.get();
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToBlockRead, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };

        final Map<String, DiskUsage> allDisksOk;
        allDisksOk = new HashMap<>();
        allDisksOk.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(15, 100)));

        final Map<String, DiskUsage> aboveLowWatermark = new HashMap<>();
        aboveLowWatermark.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(10, 14)));

        final Map<String, DiskUsage> aboveHighWatermark = new HashMap<>();
        aboveHighWatermark.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(5, 9)));

        final Map<String, DiskUsage> aboveFloodStageWatermark = new HashMap<>();
        aboveFloodStageWatermark.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, between(0, 4)));

        assertNoLogging(monitor, allDisksOk);

        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "low disk watermark [85%] exceeded on * replicas will not be assigned to this node"
        );

        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        final List<String> messages = new ArrayList<>();
        messages.add(
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );
        messages.add(
            "Putting index create block on cluster as all nodes are breaching high disk watermark. "
                + "Number of nodes above high watermark: 1."
        );
        assertMultipleWarningMessages(monitor, aboveHighWatermark, messages);

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(randomBoolean());
        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on * all indices on this node will be marked read-only"
        );

        relocatingShardSizeRef.set(-5L);
        advanceTime.set(true);

        relocatingShardSizeRef.set(0L);
        timeSupplier.getAsLong(); // advance time long enough to do another reroute
        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        assertMultipleWarningMessages(monitor, aboveHighWatermark, messages);

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(randomBoolean());
        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "high disk watermark [90%] no longer exceeded on * but low disk watermark [85%] is still exceeded"
        );

        advanceTime.set(true); // only log about dropping below the low disk watermark on a reroute
        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [85%] no longer exceeded on *");

        advanceTime.set(randomBoolean());

        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on * all indices on this node will be marked read-only"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [85%] no longer exceeded on *");

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [85%] no longer exceeded on *");

        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on * all indices on this node will be marked read-only"
        );

        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "high disk watermark [90%] no longer exceeded on * but low disk watermark [85%] is still exceeded"
        );
    }

    public void testIndexCreateBlockWhenNoDataNodeHealthy() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder().build();
        RoutingTable routingTable = RoutingTable.builder().build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .build(),
            allocation
        );
        AtomicInteger countBlocksCalled = new AtomicInteger();
        AtomicBoolean reroute = new AtomicBoolean(false);
        AtomicReference<Set<String>> indices = new AtomicReference<>();
        AtomicLong currentTime = new AtomicLong();
        Settings settings = Settings.builder().build();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            settings,
            () -> clusterState,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 5.0
        ) {

            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToBlockRead, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indices.compareAndSet(null, indicesToBlockRead));
                assertFalse(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                countBlocksCalled.set(countBlocksCalled.get() + 1);
                listener.onResponse(null);
            }
        };

        final Map<String, DiskUsage> builder = new HashMap<>();
        monitor.onNewInfo(clusterInfo(builder));
        assertTrue(countBlocksCalled.get() == 0);
    }

    public void testIndexCreateBlockRemovedOnlyWhenAnyNodeAboveHighWatermark() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.blocks.create_index.enabled", false)
                .build()
        );
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "hot_node_2"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_1")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "hot_node_1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_2")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "hot_node_1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test_1"))
            .addAsNew(metadata.index("test_2"))
            .build();

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .blocks(ClusterBlocks.builder().addGlobalBlock(Metadata.CLUSTER_CREATE_INDEX_BLOCK).build())
                .nodes(DiscoveryNodes.builder().add(newNode("hot_node_1")).add(newNode("hot_node_2")))
                .build(),
            allocation
        );
        AtomicReference<Set<String>> indices = new AtomicReference<>();
        AtomicInteger countBlocksCalled = new AtomicInteger();
        AtomicInteger countUnblockBlocksCalled = new AtomicInteger();
        AtomicLong currentTime = new AtomicLong();
        Settings settings = Settings.builder().put(CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE.getKey(), true).build();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            settings,
            () -> clusterState,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                listener.onResponse(null);
            },
            () -> 5.0
        ) {

            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToBlockRead, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                if (indexCreateBlock == true) {
                    countBlocksCalled.set(countBlocksCalled.get() + 1);
                } else {
                    countUnblockBlocksCalled.set(countUnblockBlocksCalled.get() + 1);
                }

                listener.onResponse(null);
            }
        };

        Map<String, DiskUsage> builder = new HashMap<>();

        // Initially all the nodes are breaching high watermark and IndexCreateBlock is already present on the cluster.
        // Since block is already present, DiskThresholdMonitor should not again try to apply block.
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, 9));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, 9));
        monitor.onNewInfo(clusterInfo(builder));
        // Since Block is already present and nodes are below high watermark so neither block nor unblock will be called.
        assertEquals(countBlocksCalled.get(), 0);
        assertEquals(countUnblockBlocksCalled.get(), 0);

        // Ensure DiskThresholdMonitor does not try to remove block in the next iteration if all nodes are breaching high watermark.
        monitor.onNewInfo(clusterInfo(builder));
        assertEquals(countBlocksCalled.get(), 0);
        assertEquals(countUnblockBlocksCalled.get(), 0);

        builder = new HashMap<>();

        // If any node is no longer breaching high watermark, DiskThresholdMonitor should remove IndexCreateBlock.
        builder.put("hot_node_1", new DiskUsage("hot_node_1", "hot_node_1", "/foo/bar", 100, 19));
        builder.put("hot_node_2", new DiskUsage("hot_node_2", "hot_node_2", "/foo/bar", 100, 1));
        // Need to add delay in current time to allow nodes to be removed high watermark list.
        currentTime.addAndGet(randomLongBetween(60001, 120000));

        monitor.onNewInfo(clusterInfo(builder));
        // Block will be removed if any nodes is no longer breaching high watermark.
        assertEquals(countBlocksCalled.get(), 0);
        assertEquals(countUnblockBlocksCalled.get(), 1);
    }

    public void testWarmNodeLowStageWatermarkBreach() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("remote_index")
                    .settings(
                        settings(Version.CURRENT).put("index.store.type", "remote_snapshot")
                            .put("index.routing.allocation.require._id", "warm_node")
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("remote_index")).build();

        DiscoveryNode warmNode = newNode("warm_node", Collections.singleton(DiscoveryNodeRole.WARM_ROLE));

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(warmNode))
                .build(),
            allocation
        );

        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicBoolean reroute = new AtomicBoolean(false);

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 2.0  // dataToFileCacheSizeRatio = 2
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };

        // Test warm node exceeding low stage watermark
        // Total addressable space = dataToFileCacheSizeRatio * nodeCacheSize = 2.0 * 100 = 200
        // High stage threshold (50%) = 200 * 0.15 = 30
        // Free space = 28 < 30, so should exceed low stage
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("warm_node", new DiskUsage("warm_node", "warm_node", "/foo/bar", 200, 28));

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[remote_index][0][p]", 172L);

        monitor.onNewInfo(clusterInfo(builder, Map.of(), shardSizes));

        // Should not mark remote indices read-only due to low stage breach
        // and should not trigger reroute
        assertNull(indicesToMarkReadOnly.get());
        assertFalse(reroute.get());
    }

    public void testWarmNodeHighStageWatermarkBreach() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("remote_index")
                    .settings(
                        settings(Version.CURRENT).put("index.store.type", "remote_snapshot")
                            .put("index.routing.allocation.require._id", "warm_node")
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("remote_index")).build();

        DiscoveryNode warmNode = newNode("warm_node", Collections.singleton(DiscoveryNodeRole.WARM_ROLE));

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(warmNode))
                .build(),
            allocation
        );

        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicBoolean reroute = new AtomicBoolean(false);

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 2.0  // dataToFileCacheSizeRatio = 2
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };

        // Test warm node exceeding high stage watermark
        // Total addressable space = dataToFileCacheSizeRatio * nodeCacheSize = 2.0 * 100 = 200
        // High stage threshold (10%) = 200 * 0.1 = 20
        // Free space = 18 < 20, so should exceed high stage
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("warm_node", new DiskUsage("warm_node", "warm_node", "/foo/bar", 200, 18));

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[remote_index][0][p]", 182L);

        monitor.onNewInfo(clusterInfo(builder, Map.of(), shardSizes));

        // Should not mark remote indices read-only due to High stage breach
        // but should trigger reroute
        assertNull(indicesToMarkReadOnly.get());
        assertTrue(reroute.get());
    }

    public void testWarmNodeFloodStageWatermarkBreach() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("remote_index")
                    .settings(
                        settings(Version.CURRENT).put("index.store.type", "remote_snapshot")
                            .put("index.routing.allocation.require._id", "warm_node")
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("remote_index")).build();

        DiscoveryNode warmNode = newNode("warm_node", Collections.singleton(DiscoveryNodeRole.WARM_ROLE));

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(warmNode))
                .build(),
            allocation
        );

        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> listener.onResponse(null),
            () -> 2.0  // dataToFileCacheSizeRatio = 2
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };

        // Test warm node exceeding flood stage watermark
        // Total addressable space = dataToFileCacheSizeRatio * nodeCacheSize = 2.0 * 100 = 200
        // Flood stage threshold (5%) = 200 * 0.05 = 10
        // Free space = 8 < 10, so should exceed flood stage
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("warm_node", new DiskUsage("warm_node", "warm_node", "/foo/bar", 200, 8));

        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[remote_index][0][p]", 192L);

        monitor.onNewInfo(clusterInfo(builder, Map.of(), shardSizes));

        // Should mark remote indices read-only due to flood stage breach
        assertNotNull(indicesToMarkReadOnly.get());
        assertTrue(indicesToMarkReadOnly.get().contains("remote_index"));
    }

    public void testWarmNodeFileCacheReleaseLocks() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder("remote_index")
            .settings(
                settings(Version.CURRENT).put("index.store.type", "remote_snapshot")
                    .put("index.routing.allocation.require._id", "warm_node_index_block")
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
                    .put(IndexMetadata.INDEX_BLOCKS_READ_SETTING.getKey(), true)
            )
            .numberOfShards(1)
            .numberOfReplicas(0);
        Metadata metadata = Metadata.builder().put(indexMetadataBuilder).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("remote_index")).build();
        DiscoveryNode warmNode = newNode("warm_node", Collections.singleton(DiscoveryNodeRole.WARM_ROLE));
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(warmNode))
                .blocks(ClusterBlocks.builder().addBlocks(indexMetadataBuilder.build()).build())
                .build(),
            allocation
        );
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToBlockRead = new AtomicReference<>();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 2.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                assertFalse(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                assertTrue(indicesToBlockRead.compareAndSet(null, indicesToUpdate));
                assertFalse(readBlock);
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("warm_node", new DiskUsage("warm_node", "warm_node", "/foo/bar", 200, 40));
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[remote_index][0][p]", 172L);
        monitor.onNewInfo(clusterInfo(builder, Map.of(), shardSizes));
        assertFalse(indicesToMarkReadOnly.get().isEmpty());
        assertFalse(indicesToBlockRead.get().isEmpty());
    }

    public void testWarmNodeFileCacheIndexThresholdBreach() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("remote_index")
                    .settings(
                        settings(Version.CURRENT).put("index.store.type", "remote_snapshot")
                            .put("index.routing.allocation.require._id", "warm_node_index_block")
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("remote_index")).build();
        DiscoveryNode warmNode = newNode("warm_node_index_block", Collections.singleton(DiscoveryNodeRole.WARM_ROLE));
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(warmNode))
                .build(),
            allocation
        );
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToBlockRead = new AtomicReference<>();
        AtomicBoolean reroute = new AtomicBoolean(false);
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 2.0
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                assertFalse(readBlock);
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("warm_node_index_block", new DiskUsage("warm_node_index_block", "warm_node_index_block", "/foo/bar", 200, 40));
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[remote_index][0][p]", 172L);
        monitor.onNewInfo(clusterInfo(builder, Map.of(), shardSizes));
        assertFalse(indicesToMarkReadOnly.get().isEmpty());
        assertNull(indicesToBlockRead.get());
    }

    public void testWarmNodeFileCacheSearchThresholdBreach() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("remote_index")
                    .settings(
                        settings(Version.CURRENT).put("index.store.type", "remote_snapshot")
                            .put("index.routing.allocation.require._id", "warm_node_search_block")
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("remote_index")).build();
        DiscoveryNode warmNode = newNode("warm_node_search_block", Collections.singleton(DiscoveryNodeRole.WARM_ROLE));
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(warmNode))
                .build(),
            allocation
        );
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToBlockRead = new AtomicReference<>();
        AtomicBoolean reroute = new AtomicBoolean(false);
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            () -> 2.0  // dataToFileCacheSizeRatio = 2
        ) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                assertTrue(readOnly);
                listener.onResponse(null);
            }

            @Override
            protected void updateIndicesReadBlock(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readBlock) {
                assertTrue(indicesToBlockRead.compareAndSet(null, indicesToUpdate));
                assertTrue(readBlock);
                listener.onResponse(null);
            }

            @Override
            protected void setIndexCreateBlock(ActionListener<Void> listener, boolean indexCreateBlock) {
                listener.onResponse(null);
            }
        };
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put("warm_node_search_block", new DiskUsage("warm_node_search_block", "warm_node_search_block", "/foo/bar", 200, 40));
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[remote_index][0][p]", 172L);
        monitor.onNewInfo(clusterInfo(builder, Map.of(), shardSizes));
        assertFalse(indicesToMarkReadOnly.get().isEmpty());
        assertFalse(indicesToBlockRead.get().isEmpty());
    }

    private void assertNoLogging(DiskThresholdMonitor monitor, final Map<String, DiskUsage> diskUsages) throws IllegalAccessException {
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(DiskThresholdMonitor.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "any INFO message",
                    DiskThresholdMonitor.class.getCanonicalName(),
                    Level.INFO,
                    "*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "any WARN message",
                    DiskThresholdMonitor.class.getCanonicalName(),
                    Level.WARN,
                    "*"
                )
            );

            for (int i = between(1, 3); i >= 0; i--) {
                monitor.onNewInfo(clusterInfo(diskUsages));
            }

            mockAppender.assertAllExpectationsMatched();
        }
    }

    private void assertRepeatedWarningMessages(DiskThresholdMonitor monitor, final Map<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        for (int i = between(1, 3); i >= 0; i--) {
            assertLogging(monitor, diskUsages, Level.WARN, message);
        }
    }

    private void assertMultipleWarningMessages(DiskThresholdMonitor monitor, final Map<String, DiskUsage> diskUsages, List<String> messages)
        throws IllegalAccessException {
        for (int index = 0; index < messages.size(); index++) {
            assertLogging(monitor, diskUsages, Level.WARN, messages.get(index));
        }
    }

    private void assertSingleInfoMessage(DiskThresholdMonitor monitor, final Map<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        assertLogging(monitor, diskUsages, Level.INFO, message);
        assertNoLogging(monitor, diskUsages);
    }

    private void assertLogging(DiskThresholdMonitor monitor, final Map<String, DiskUsage> diskUsages, Level level, String message)
        throws IllegalAccessException {
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(DiskThresholdMonitor.class))) {
            mockAppender.start();
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation("expected message", DiskThresholdMonitor.class.getCanonicalName(), level, message)
            );
            mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "any message of another level",
                    DiskThresholdMonitor.class.getCanonicalName(),
                    level == Level.INFO ? Level.WARN : Level.INFO,
                    "*"
                )
            );

            monitor.onNewInfo(clusterInfo(diskUsages));

            mockAppender.assertAllExpectationsMatched();
        }
    }

    private static ClusterInfo clusterInfo(final Map<String, DiskUsage> diskUsages) {
        return clusterInfo(diskUsages, Map.of(), Map.of());
    }

    private static ClusterInfo clusterInfo(
        final Map<String, DiskUsage> diskUsages,
        final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace,
        final Map<String, Long> shardSizes
    ) {
        final Map<String, AggregateFileCacheStats> fileCacheStats = new HashMap<>();
        diskUsages.forEach((key, value) -> {
            if (key.contains("warm")) {
                if (key.contains("search_block")) {
                    fileCacheStats.put(key, createAggregateFileCacheStats(100, between(100, 110), 110));
                } else if (key.contains("index_block")) {
                    fileCacheStats.put(key, createAggregateFileCacheStats(100, between(90, 99), 99));
                } else {
                    fileCacheStats.put(key, createAggregateFileCacheStats(100, between(0, 89), 89));
                }
            }
        });
        return new ClusterInfo(diskUsages, null, shardSizes, null, reservedSpace, fileCacheStats, Map.of());
    }

    private static AggregateFileCacheStats createAggregateFileCacheStats(long totalCacheSize, long active, long used) {
        FileCacheStats overallStats = new FileCacheStats(
            active,
            totalCacheSize,
            used,
            0,
            0,
            0,
            0,
            AggregateFileCacheStats.FileCacheStatsType.OVER_ALL_STATS
        );
        FileCacheStats fullStats = new FileCacheStats(
            0,
            totalCacheSize,
            0,
            0,
            0,
            0,
            0,
            AggregateFileCacheStats.FileCacheStatsType.FULL_FILE_STATS
        );
        FileCacheStats blockStats = new FileCacheStats(
            0,
            totalCacheSize,
            0,
            0,
            0,
            0,
            0,
            AggregateFileCacheStats.FileCacheStatsType.BLOCK_FILE_STATS
        );
        FileCacheStats pinnedStats = new FileCacheStats(
            0,
            totalCacheSize,
            0,
            0,
            0,
            0,
            0,
            AggregateFileCacheStats.FileCacheStatsType.PINNED_FILE_STATS
        );
        return new AggregateFileCacheStats(System.currentTimeMillis(), overallStats, fullStats, blockStats, pinnedStats);
    }

}
