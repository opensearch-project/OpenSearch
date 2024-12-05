/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequestBuilder;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.coordination.LeaderChecker;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreStatsIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    public void setup() {
        internalCluster().startNodes(3);
    }

    public void testStatsResponseFromAllNodes() {
        setup();

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find all the nodes that are present in the cluster. We make the remote store stats api call from
        // each of the node in the cluster and check that the response is coming as expected.
        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toList());
        String shardId = "0";
        for (String node : nodes) {
            RemoteStoreStatsResponse response = client(node).admin().cluster().prepareRemoteStoreStats(INDEX_NAME, shardId).get();
            assertTrue(response.getSuccessfulShards() > 0);
            assertTrue(response.getRemoteStoreStats() != null && response.getRemoteStoreStats().length != 0);
            final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, shardId);
            List<RemoteStoreStats> matches = Arrays.stream(response.getRemoteStoreStats())
                .filter(stat -> indexShardId.equals(stat.getSegmentStats().shardId.toString()))
                .collect(Collectors.toList());
            assertEquals(1, matches.size());

            RemoteSegmentTransferTracker.Stats segmentStats = matches.get(0).getSegmentStats();
            validateSegmentUploadStats(segmentStats);
            assertEquals(0, segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted);

            RemoteTranslogTransferTracker.Stats translogStats = matches.get(0).getTranslogStats();
            assertNonZeroTranslogUploadStatsNoFailures(translogStats);
            assertZeroTranslogDownloadStats(translogStats);
        }

        // Step 3 - Enable replicas on the existing indices and ensure that download
        // stats are being populated as well
        changeReplicaCountAndEnsureGreen(1);
        for (String node : nodes) {
            RemoteStoreStatsResponse response = client(node).admin().cluster().prepareRemoteStoreStats(INDEX_NAME, shardId).get();
            assertTrue(response.getSuccessfulShards() > 0);
            assertTrue(response.getRemoteStoreStats() != null && response.getRemoteStoreStats().length != 0);
            final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, shardId);
            List<RemoteStoreStats> matches = Arrays.stream(response.getRemoteStoreStats())
                .filter(stat -> indexShardId.equals(stat.getSegmentStats().shardId.toString()))
                .collect(Collectors.toList());
            assertEquals(2, matches.size());
            for (RemoteStoreStats stat : matches) {
                ShardRouting routing = stat.getShardRouting();
                validateShardRouting(routing);
                RemoteSegmentTransferTracker.Stats segmentStats = stat.getSegmentStats();
                RemoteTranslogTransferTracker.Stats translogStats = stat.getTranslogStats();
                if (routing.primary()) {
                    validateSegmentUploadStats(segmentStats);
                    assertEquals(0, segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted);

                    assertNonZeroTranslogUploadStatsNoFailures(translogStats);
                    assertZeroTranslogDownloadStats(translogStats);
                } else {
                    validateSegmentDownloadStats(segmentStats);
                    assertEquals(0, segmentStats.totalUploadsStarted);

                    assertZeroTranslogUploadStats(translogStats);
                    assertZeroTranslogDownloadStats(translogStats);
                }
            }
        }
    }

    public void testStatsResponseAllShards() {
        setup();

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find all the nodes that are present in the cluster. We make the remote store stats api call from
        // each of the node in the cluster and check that the response is coming as expected.
        ClusterState state = getClusterState();
        String node = state.nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).findFirst().get();
        RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = client(node).admin()
            .cluster()
            .prepareRemoteStoreStats(INDEX_NAME, null);
        RemoteStoreStatsResponse response = remoteStoreStatsRequestBuilder.get();
        assertEquals(3, response.getSuccessfulShards());
        assertTrue(response.getRemoteStoreStats() != null && response.getRemoteStoreStats().length == 3);

        RemoteSegmentTransferTracker.Stats segmentStats = response.getRemoteStoreStats()[0].getSegmentStats();
        validateSegmentUploadStats(segmentStats);
        assertEquals(0, segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted);

        RemoteTranslogTransferTracker.Stats translogStats = response.getRemoteStoreStats()[0].getTranslogStats();
        assertNonZeroTranslogUploadStatsNoFailures(translogStats);
        assertZeroTranslogDownloadStats(translogStats);

        // Step 3 - Enable replicas on the existing indices and ensure that download
        // stats are being populated as well
        changeReplicaCountAndEnsureGreen(1);
        response = client(node).admin().cluster().prepareRemoteStoreStats(INDEX_NAME, null).get();
        assertEquals(6, response.getSuccessfulShards());
        assertTrue(response.getRemoteStoreStats() != null && response.getRemoteStoreStats().length == 6);
        for (RemoteStoreStats stat : response.getRemoteStoreStats()) {
            ShardRouting routing = stat.getShardRouting();
            validateShardRouting(routing);
            segmentStats = stat.getSegmentStats();
            translogStats = stat.getTranslogStats();
            if (routing.primary()) {
                validateSegmentUploadStats(segmentStats);
                assertEquals(0, segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted);

                assertNonZeroTranslogUploadStatsNoFailures(translogStats);
                assertZeroTranslogDownloadStats(translogStats);
            } else {
                validateSegmentDownloadStats(segmentStats);
                assertEquals(0, segmentStats.totalUploadsStarted);

                assertZeroTranslogUploadStats(translogStats);
                assertZeroTranslogDownloadStats(translogStats);
            }
        }

    }

    public void testStatsResponseFromLocalNode() {
        setup();

        // Step 1 - We create cluster, create an index, and then index documents into. We also do multiple refreshes/flushes
        // during this time frame. This ensures that the segment upload has started.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexDocs();

        // Step 2 - We find a data node in the cluster. We make the remote store stats api call from
        // each of the data node in the cluster and check that only local shards are returned.
        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toList());
        for (String node : nodes) {
            RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = client(node).admin()
                .cluster()
                .prepareRemoteStoreStats(INDEX_NAME, null);
            remoteStoreStatsRequestBuilder.setLocal(true);
            RemoteStoreStatsResponse response = remoteStoreStatsRequestBuilder.get();
            assertEquals(1, response.getSuccessfulShards());
            assertTrue(response.getRemoteStoreStats() != null && response.getRemoteStoreStats().length == 1);
            RemoteSegmentTransferTracker.Stats segmentStats = response.getRemoteStoreStats()[0].getSegmentStats();
            validateSegmentUploadStats(segmentStats);
            assertEquals(0, segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted);

            RemoteTranslogTransferTracker.Stats translogStats = response.getRemoteStoreStats()[0].getTranslogStats();
            assertNonZeroTranslogUploadStatsNoFailures(translogStats);
            assertZeroTranslogDownloadStats(translogStats);
        }
        changeReplicaCountAndEnsureGreen(1);
        for (String node : nodes) {
            RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = client(node).admin()
                .cluster()
                .prepareRemoteStoreStats(INDEX_NAME, null);
            remoteStoreStatsRequestBuilder.setLocal(true);
            RemoteStoreStatsResponse response = remoteStoreStatsRequestBuilder.get();
            assertTrue(response.getSuccessfulShards() > 0);
            assertTrue(response.getRemoteStoreStats() != null && response.getRemoteStoreStats().length != 0);
            for (RemoteStoreStats stat : response.getRemoteStoreStats()) {
                ShardRouting routing = stat.getShardRouting();
                validateShardRouting(routing);
                RemoteSegmentTransferTracker.Stats segmentStats = stat.getSegmentStats();
                RemoteTranslogTransferTracker.Stats translogStats = stat.getTranslogStats();
                if (routing.primary()) {
                    validateSegmentUploadStats(segmentStats);
                    assertEquals(0, segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted);

                    assertNonZeroTranslogUploadStatsNoFailures(translogStats);
                    assertZeroTranslogDownloadStats(translogStats);
                } else {
                    validateSegmentDownloadStats(segmentStats);
                    assertEquals(0, segmentStats.totalUploadsStarted);

                    assertZeroTranslogUploadStats(translogStats);
                    assertZeroTranslogDownloadStats(translogStats);
                }
            }
        }
    }

    @TestLogging(reason = "Getting trace logs from remote store package", value = "org.opensearch.index.shard:TRACE")
    public void testDownloadStatsCorrectnessSinglePrimarySingleReplica() throws Exception {
        setup();
        // Scenario:
        // - Create index with single primary and single replica shard
        // - Disable Refresh Interval for the index
        // - Index documents
        // - Trigger refresh and flush
        // - Assert that download stats == upload stats
        // - Repeat this step for random times (between 5 and 10)

        // Create index with 1 pri and 1 replica and refresh interval disabled
        createIndex(
            INDEX_NAME,
            Settings.builder().put(remoteStoreIndexSettings(1, 1)).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build()
        );
        ensureGreen(INDEX_NAME);

        // Manually invoke a refresh
        refresh(INDEX_NAME);

        // Get zero state values
        // Extract and assert zero state primary stats
        RemoteStoreStatsResponse zeroStateResponse = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
        RemoteSegmentTransferTracker.Stats zeroStatePrimaryStats = Arrays.stream(zeroStateResponse.getRemoteStoreStats())
            .filter(remoteStoreStats -> remoteStoreStats.getShardRouting().primary())
            .collect(Collectors.toList())
            .get(0)
            .getSegmentStats();
        logger.info(
            "Zero state primary stats: {}ms refresh time lag, {}b bytes lag, {}b upload bytes started, {}b upload bytes failed , {} uploads succeeded, {} upload byes succeeded.",
            zeroStatePrimaryStats.refreshTimeLagMs,
            zeroStatePrimaryStats.bytesLag,
            zeroStatePrimaryStats.uploadBytesStarted,
            zeroStatePrimaryStats.uploadBytesFailed,
            zeroStatePrimaryStats.totalUploadsSucceeded,
            zeroStatePrimaryStats.uploadBytesSucceeded
        );
        assertTrue(
            zeroStatePrimaryStats.totalUploadsStarted == zeroStatePrimaryStats.totalUploadsSucceeded
                && zeroStatePrimaryStats.totalUploadsSucceeded == 1
        );
        assertTrue(
            zeroStatePrimaryStats.uploadBytesStarted == zeroStatePrimaryStats.uploadBytesSucceeded
                && zeroStatePrimaryStats.uploadBytesSucceeded > 0
        );
        assertTrue(zeroStatePrimaryStats.totalUploadsFailed == 0 && zeroStatePrimaryStats.uploadBytesFailed == 0);

        // Extract and assert zero state replica stats
        RemoteSegmentTransferTracker.Stats zeroStateReplicaStats = Arrays.stream(zeroStateResponse.getRemoteStoreStats())
            .filter(remoteStoreStats -> !remoteStoreStats.getShardRouting().primary())
            .collect(Collectors.toList())
            .get(0)
            .getSegmentStats();
        assertTrue(
            zeroStateReplicaStats.directoryFileTransferTrackerStats.transferredBytesStarted == 0
                && zeroStateReplicaStats.directoryFileTransferTrackerStats.transferredBytesSucceeded == 0
        );

        // Index documents
        for (int i = 1; i <= randomIntBetween(5, 10); i++) {
            indexSingleDoc(INDEX_NAME);
            // Running Flush & Refresh manually
            flushAndRefresh(INDEX_NAME);
            ensureGreen(INDEX_NAME);

            // Poll for RemoteStore Stats
            assertBusy(() -> {
                RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
                // Iterate through the response and extract the relevant segment upload and download stats
                List<RemoteStoreStats> primaryStatsList = Arrays.stream(response.getRemoteStoreStats())
                    .filter(remoteStoreStats -> remoteStoreStats.getShardRouting().primary())
                    .collect(Collectors.toList());
                assertEquals(1, primaryStatsList.size());
                List<RemoteStoreStats> replicaStatsList = Arrays.stream(response.getRemoteStoreStats())
                    .filter(remoteStoreStats -> !remoteStoreStats.getShardRouting().primary())
                    .collect(Collectors.toList());
                assertEquals(1, replicaStatsList.size());
                RemoteSegmentTransferTracker.Stats primaryStats = primaryStatsList.get(0).getSegmentStats();
                RemoteSegmentTransferTracker.Stats replicaStats = replicaStatsList.get(0).getSegmentStats();
                // Assert Upload syncs - zero state uploads == download syncs
                assertTrue(primaryStats.totalUploadsStarted > 0);
                assertTrue(primaryStats.totalUploadsSucceeded > 0);
                assertTrue(
                    replicaStats.directoryFileTransferTrackerStats.transferredBytesStarted > 0
                        && primaryStats.uploadBytesStarted
                            - zeroStatePrimaryStats.uploadBytesStarted >= replicaStats.directoryFileTransferTrackerStats.transferredBytesStarted
                );
                assertTrue(
                    replicaStats.directoryFileTransferTrackerStats.transferredBytesSucceeded > 0
                        && primaryStats.uploadBytesSucceeded
                            - zeroStatePrimaryStats.uploadBytesSucceeded >= replicaStats.directoryFileTransferTrackerStats.transferredBytesSucceeded
                );
                // Assert zero failures
                assertEquals(0, primaryStats.uploadBytesFailed);
                assertEquals(0, replicaStats.directoryFileTransferTrackerStats.transferredBytesFailed);
            }, 60, TimeUnit.SECONDS);
        }
    }

    @TestLogging(reason = "Getting trace logs from remote store package", value = "org.opensearch.index.shard:TRACE")
    public void testDownloadStatsCorrectnessSinglePrimaryMultipleReplicaShards() throws Exception {
        setup();
        // Scenario:
        // - Create index with single primary and N-1 replica shards (N = no of data nodes)
        // - Disable Refresh Interval for the index
        // - Index documents
        // - Trigger refresh and flush
        // - Assert that download stats == upload stats
        // - Repeat this step for random times (between 5 and 10)

        // Create index
        int dataNodeCount = client().admin().cluster().prepareHealth().get().getNumberOfDataNodes();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(remoteStoreIndexSettings(dataNodeCount - 1, 1))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .build()
        );
        ensureGreen(INDEX_NAME);

        // Manually invoke a refresh
        refresh(INDEX_NAME);

        // Get zero state values
        // Extract and assert zero state primary stats
        RemoteStoreStatsResponse zeroStateResponse = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
        RemoteSegmentTransferTracker.Stats zeroStatePrimaryStats = Arrays.stream(zeroStateResponse.getRemoteStoreStats())
            .filter(remoteStoreStats -> remoteStoreStats.getShardRouting().primary())
            .collect(Collectors.toList())
            .get(0)
            .getSegmentStats();
        logger.info(
            "Zero state primary stats: {}ms refresh time lag, {}b bytes lag, {}b upload bytes started, {}b upload bytes failed , {} uploads succeeded, {} upload byes succeeded.",
            zeroStatePrimaryStats.refreshTimeLagMs,
            zeroStatePrimaryStats.bytesLag,
            zeroStatePrimaryStats.uploadBytesStarted,
            zeroStatePrimaryStats.uploadBytesFailed,
            zeroStatePrimaryStats.totalUploadsSucceeded,
            zeroStatePrimaryStats.uploadBytesSucceeded
        );
        assertTrue(
            zeroStatePrimaryStats.totalUploadsStarted == zeroStatePrimaryStats.totalUploadsSucceeded
                && zeroStatePrimaryStats.totalUploadsSucceeded == 1
        );
        assertTrue(
            zeroStatePrimaryStats.uploadBytesStarted == zeroStatePrimaryStats.uploadBytesSucceeded
                && zeroStatePrimaryStats.uploadBytesSucceeded > 0
        );
        assertTrue(zeroStatePrimaryStats.totalUploadsFailed == 0 && zeroStatePrimaryStats.uploadBytesFailed == 0);

        // Extract and assert zero state replica stats
        List<RemoteStoreStats> zeroStateReplicaStats = Arrays.stream(zeroStateResponse.getRemoteStoreStats())
            .filter(remoteStoreStats -> !remoteStoreStats.getShardRouting().primary())
            .collect(Collectors.toList());
        zeroStateReplicaStats.forEach(stats -> {
            assertTrue(
                stats.getSegmentStats().directoryFileTransferTrackerStats.transferredBytesStarted == 0
                    && stats.getSegmentStats().directoryFileTransferTrackerStats.transferredBytesSucceeded == 0
            );
        });

        int currentNodesInCluster = client().admin().cluster().prepareHealth().get().getNumberOfDataNodes();
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            indexSingleDoc(INDEX_NAME);
            // Running Flush & Refresh manually
            flushAndRefresh(INDEX_NAME);

            assertBusy(() -> {
                RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
                assertEquals(currentNodesInCluster, response.getSuccessfulShards());
                long uploadsStarted = 0, uploadsSucceeded = 0, uploadsFailed = 0;
                long uploadBytesStarted = 0, uploadBytesSucceeded = 0, uploadBytesFailed = 0;
                List<Long> downloadBytesStarted = new ArrayList<>(), downloadBytesSucceeded = new ArrayList<>(), downloadBytesFailed =
                    new ArrayList<>();

                // Assert that stats for primary shard and replica shard set are equal
                for (RemoteStoreStats eachStatsObject : response.getRemoteStoreStats()) {
                    RemoteSegmentTransferTracker.Stats stats = eachStatsObject.getSegmentStats();
                    if (eachStatsObject.getShardRouting().primary()) {
                        uploadBytesStarted = stats.uploadBytesStarted;
                        uploadBytesSucceeded = stats.uploadBytesSucceeded;
                        uploadBytesFailed = stats.uploadBytesFailed;
                    } else {
                        downloadBytesStarted.add(stats.directoryFileTransferTrackerStats.transferredBytesStarted);
                        downloadBytesSucceeded.add(stats.directoryFileTransferTrackerStats.transferredBytesSucceeded);
                        downloadBytesFailed.add(stats.directoryFileTransferTrackerStats.transferredBytesFailed);
                    }
                }

                assertEquals(0, uploadsFailed);
                assertEquals(0, uploadBytesFailed);
                for (int j = 0; j < response.getSuccessfulShards() - 1; j++) {
                    assertTrue(uploadBytesStarted - zeroStatePrimaryStats.uploadBytesStarted > downloadBytesStarted.get(j));
                    assertTrue(uploadBytesSucceeded - zeroStatePrimaryStats.uploadBytesSucceeded > downloadBytesSucceeded.get(j));
                    assertEquals(0, (long) downloadBytesFailed.get(j));
                }
            }, 60, TimeUnit.SECONDS);
        }
    }

    public void testStatsOnShardRelocation() {
        setup();
        // Scenario:
        // - Create index with single primary and single replica shard
        // - Index documents
        // - Reroute replica shard to one of the remaining nodes
        // - Assert that remote store stats reflects the new node ID

        // Create index
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 1));
        ensureGreen(INDEX_NAME);
        // Index docs
        indexDocs();

        // Fetch current set of nodes in the cluster
        List<String> currentNodesInCluster = getClusterState().nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getId)
            .collect(Collectors.toList());
        DiscoveryNode[] discoveryNodesForIndex = client().admin().cluster().prepareSearchShards(INDEX_NAME).get().getNodes();

        // Fetch nodes with shard copies of the created index
        List<String> nodeIdsWithShardCopies = new ArrayList<>();
        Arrays.stream(discoveryNodesForIndex).forEach(eachNode -> nodeIdsWithShardCopies.add(eachNode.getId()));

        // Fetch nodes which does not have any copies of the index
        List<String> nodeIdsWithoutShardCopy = currentNodesInCluster.stream()
            .filter(eachNode -> !nodeIdsWithShardCopies.contains(eachNode))
            .collect(Collectors.toList());
        assertEquals(1, nodeIdsWithoutShardCopy.size());

        // Manually reroute shard to a node which does not have any shard copy at present
        ShardRouting replicaShardRouting = getClusterState().routingTable()
            .index(INDEX_NAME)
            .shard(0)
            .assignedShards()
            .stream()
            .filter(shard -> !shard.primary())
            .collect(Collectors.toList())
            .get(0);
        String sourceNode = replicaShardRouting.currentNodeId();
        String destinationNode = nodeIdsWithoutShardCopy.get(0);
        relocateShard(0, sourceNode, destinationNode);
        RemoteStoreStats[] allShardsStats = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get().getRemoteStoreStats();
        RemoteStoreStats replicaShardStat = Arrays.stream(allShardsStats)
            .filter(eachStat -> !eachStat.getShardRouting().primary())
            .collect(Collectors.toList())
            .get(0);

        // Assert that remote store stats reflect the new shard state
        assertEquals(ShardRoutingState.STARTED, replicaShardStat.getShardRouting().state());
        assertEquals(destinationNode, replicaShardStat.getShardRouting().currentNodeId());
    }

    public void testStatsOnShardUnassigned() throws IOException {
        setup();
        // Scenario:
        // - Create index with single primary and two replica shard
        // - Index documents
        // - Stop one data node
        // - Assert:
        // a. Total shard Count in the response object is equal to the previous node count
        // b. Successful shard count in the response object is equal to the new node count
        createIndex(INDEX_NAME, remoteStoreIndexSettings(2, 1));
        ensureGreen(INDEX_NAME);
        indexDocs();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().get();
        int dataNodeCountBeforeStop = clusterHealthResponse.getNumberOfDataNodes();
        int nodeCount = clusterHealthResponse.getNumberOfNodes();
        String nodeToBeStopped = randomBoolean() ? primaryNodeName(INDEX_NAME) : replicaNodeName(INDEX_NAME);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeToBeStopped));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureStableCluster(nodeCount - 1);
        RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
        int dataNodeCountAfterStop = client().admin().cluster().prepareHealth().get().getNumberOfDataNodes();
        assertEquals(dataNodeCountBeforeStop, response.getTotalShards());
        assertEquals(dataNodeCountAfterStop, response.getSuccessfulShards());
        // Indexing docs to ensure that the primary has started
        indexSingleDoc(INDEX_NAME);
    }

    public void testStatsOnRemoteStoreRestore() throws IOException {
        setup();
        // Creating an index with primary shard count == total nodes in cluster and 0 replicas
        int dataNodeCount = client().admin().cluster().prepareHealth().get().getNumberOfDataNodes();
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, dataNodeCount));
        ensureGreen(INDEX_NAME);

        // Index some docs to ensure segments being uploaded to remote store
        indexDocs();
        refresh(INDEX_NAME);

        // Stop one data node to force the index into a red state
        internalCluster().stopRandomDataNode();
        ensureRed(INDEX_NAME);

        // Start another data node to fulfil the previously launched capacity
        internalCluster().startDataOnlyNode();

        // Restore index from remote store
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());

        // Ensure that the index is green
        ensureGreen(INDEX_NAME);

        // Index some more docs to force segment uploads to remote store
        indexDocs();

        RemoteStoreStatsResponse remoteStoreStatsResponse = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
        Arrays.stream(remoteStoreStatsResponse.getRemoteStoreStats()).forEach(statObject -> {
            RemoteSegmentTransferTracker.Stats segmentStats = statObject.getSegmentStats();
            // Assert that we have both upload and download stats for the index
            assertTrue(
                segmentStats.totalUploadsStarted > 0 && segmentStats.totalUploadsSucceeded > 0 && segmentStats.totalUploadsFailed == 0
            );
            assertTrue(
                segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted > 0
                    && segmentStats.directoryFileTransferTrackerStats.transferredBytesSucceeded > 0
            );

            RemoteTranslogTransferTracker.Stats translogStats = statObject.getTranslogStats();
            assertNonZeroTranslogUploadStatsNoFailures(translogStats);
            assertNonZeroTranslogDownloadStats(translogStats);
        });
    }

    public void testNonZeroPrimaryStatsOnNewlyCreatedIndexWithZeroDocs() throws Exception {
        setup();
        // Create an index with one primary and one replica shard
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 1));
        ensureGreen(INDEX_NAME);
        refresh(INDEX_NAME);

        // Ensure that the index has 0 documents in it
        assertEquals(0, client().admin().indices().prepareStats(INDEX_NAME).get().getTotal().docs.getCount());

        // Assert that within 5 seconds the download and upload stats moves to a non-zero value
        assertBusy(() -> {
            RemoteStoreStats[] remoteStoreStats = client().admin()
                .cluster()
                .prepareRemoteStoreStats(INDEX_NAME, "0")
                .get()
                .getRemoteStoreStats();
            Arrays.stream(remoteStoreStats).forEach(statObject -> {
                RemoteSegmentTransferTracker.Stats segmentStats = statObject.getSegmentStats();
                RemoteTranslogTransferTracker.Stats translogStats = statObject.getTranslogStats();
                if (statObject.getShardRouting().primary()) {
                    assertTrue(
                        segmentStats.totalUploadsSucceeded == 1
                            && segmentStats.totalUploadsStarted == segmentStats.totalUploadsSucceeded
                            && segmentStats.totalUploadsFailed == 0
                    );
                    // On primary shard creation, we upload to remote translog post primary mode activation.
                    // This changes upload stats to non-zero for primary shard.
                    assertNonZeroTranslogUploadStatsNoFailures(translogStats);
                } else {
                    assertTrue(
                        segmentStats.directoryFileTransferTrackerStats.transferredBytesStarted == 0
                            && segmentStats.directoryFileTransferTrackerStats.transferredBytesSucceeded == 0
                    );
                    assertZeroTranslogUploadStats(translogStats);
                }
                assertZeroTranslogDownloadStats(translogStats);
            });
        }, 10, TimeUnit.SECONDS);
    }

    public void testStatsCorrectnessOnFailover() {
        Settings clusterSettings = Settings.builder()
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "100ms")
            .put(LeaderChecker.LEADER_CHECK_INTERVAL_SETTING.getKey(), "500ms")
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "500ms")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(nodeSettings(0))
            .build();
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(clusterSettings);
        internalCluster().startDataOnlyNodes(2, clusterSettings);

        // Create an index with one primary and one replica shard
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 1));
        ensureGreen(INDEX_NAME);

        // Index some docs and refresh
        indexDocs();
        refresh(INDEX_NAME);

        String primaryNode = primaryNodeName(INDEX_NAME);
        String replicaNode = replicaNodeName(INDEX_NAME);

        // Start network disruption - primary node will be isolated
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode, replicaNode).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(primaryNode).collect(Collectors.toCollection(HashSet::new));
        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();
        ensureStableCluster(2, clusterManagerNode);

        RemoteStoreStatsResponse response = client(clusterManagerNode).admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
        final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, "0");
        List<RemoteStoreStats> matches = Arrays.stream(response.getRemoteStoreStats())
            .filter(stat -> indexShardId.equals(stat.getSegmentStats().shardId.toString()))
            .collect(Collectors.toList());
        assertEquals(1, matches.size());
        RemoteSegmentTransferTracker.Stats segmentStats = matches.get(0).getSegmentStats();
        assertEquals(0, segmentStats.refreshTimeLagMs);

        networkDisruption.stopDisrupting();
        internalCluster().clearDisruptionScheme();
        ensureStableCluster(3, clusterManagerNode);
        ensureGreen(INDEX_NAME);
        logger.info("Test completed");
    }

    public void testZeroLagOnCreateIndex() throws InterruptedException {
        setup();
        String clusterManagerNode = internalCluster().getClusterManagerName();

        int numOfShards = randomIntBetween(1, 3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, numOfShards));
        ensureGreen(INDEX_NAME);
        long currentTimeNs = System.nanoTime();
        while (currentTimeNs == System.nanoTime()) {
            Thread.sleep(10);
        }

        for (int i = 0; i < numOfShards; i++) {
            RemoteStoreStatsResponse response = client(clusterManagerNode).admin()
                .cluster()
                .prepareRemoteStoreStats(INDEX_NAME, String.valueOf(i))
                .get();
            for (RemoteStoreStats remoteStoreStats : response.getRemoteStoreStats()) {
                assertEquals(0, remoteStoreStats.getSegmentStats().refreshTimeLagMs);
            }
        }
    }

    private void indexDocs() {
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            if (randomBoolean()) {
                flush(INDEX_NAME);
            } else {
                refresh(INDEX_NAME);
            }
            int numberOfOperations = randomIntBetween(10, 30);
            indexBulk(INDEX_NAME, numberOfOperations);
        }
    }

    private void changeReplicaCountAndEnsureGreen(int replicaCount) {
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount))
        );
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }

    private void relocateShard(int shardId, String sourceNode, String destNode) {
        assertAcked(client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX_NAME, shardId, sourceNode, destNode)));
        ensureGreen(INDEX_NAME);
    }

    private void validateSegmentUploadStats(RemoteSegmentTransferTracker.Stats stats) {
        assertEquals(0, stats.refreshTimeLagMs);
        assertEquals(stats.localRefreshNumber, stats.remoteRefreshNumber);
        assertTrue(stats.uploadBytesStarted > 0);
        assertEquals(0, stats.uploadBytesFailed);
        assertTrue(stats.uploadBytesSucceeded > 0);
        assertTrue(stats.totalUploadsStarted > 0);
        assertEquals(0, stats.totalUploadsFailed);
        assertTrue(stats.totalUploadsSucceeded > 0);
        assertEquals(0, stats.rejectionCount);
        assertEquals(0, stats.consecutiveFailuresCount);
        assertEquals(0, stats.bytesLag);
        assertTrue(stats.uploadBytesMovingAverage > 0);
        assertTrue(stats.uploadBytesPerSecMovingAverage > 0);
        assertTrue(stats.uploadTimeMovingAverage > 0);
    }

    private void validateSegmentDownloadStats(RemoteSegmentTransferTracker.Stats stats) {
        assertTrue(stats.directoryFileTransferTrackerStats.lastTransferTimestampMs > 0);
        assertTrue(stats.directoryFileTransferTrackerStats.transferredBytesStarted > 0);
        assertTrue(stats.directoryFileTransferTrackerStats.transferredBytesSucceeded > 0);
        assertEquals(stats.directoryFileTransferTrackerStats.transferredBytesFailed, 0);
        assertTrue(stats.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes > 0);
        assertTrue(stats.directoryFileTransferTrackerStats.transferredBytesMovingAverage > 0);
        assertTrue(stats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage > 0);
    }

    private void assertNonZeroTranslogUploadStatsNoFailures(RemoteTranslogTransferTracker.Stats stats) {
        assertTrue(stats.uploadBytesStarted > 0);
        assertTrue(stats.totalUploadsStarted > 0);
        assertEquals(0, stats.uploadBytesFailed);
        assertEquals(0, stats.totalUploadsFailed);
        assertTrue(stats.uploadBytesSucceeded > 0);
        assertTrue(stats.totalUploadsSucceeded > 0);
        assertTrue(stats.totalUploadTimeInMillis > 0);
        assertTrue(stats.lastSuccessfulUploadTimestamp > 0);
    }

    private void assertZeroTranslogUploadStats(RemoteTranslogTransferTracker.Stats stats) {
        assertEquals(0, stats.uploadBytesStarted);
        assertEquals(0, stats.totalUploadsStarted);
        assertEquals(0, stats.uploadBytesFailed);
        assertEquals(0, stats.totalUploadsFailed);
        assertEquals(0, stats.uploadBytesSucceeded);
        assertEquals(0, stats.totalUploadsSucceeded);
        assertEquals(0, stats.totalUploadTimeInMillis);
        assertEquals(0, stats.lastSuccessfulUploadTimestamp);
    }

    private void assertNonZeroTranslogDownloadStats(RemoteTranslogTransferTracker.Stats stats) {
        assertTrue(stats.downloadBytesSucceeded > 0);
        assertTrue(stats.totalDownloadsSucceeded > 0);
        // TODO: Need to simulate a delay for this assertion to avoid flakiness
        // assertTrue(stats.totalDownloadTimeInMillis > 0);
        assertTrue(stats.lastSuccessfulDownloadTimestamp > 0);
    }

    private void assertZeroTranslogDownloadStats(RemoteTranslogTransferTracker.Stats stats) {
        assertEquals(0, stats.downloadBytesSucceeded);
        assertEquals(0, stats.totalDownloadsSucceeded);
        assertEquals(0, stats.totalDownloadTimeInMillis);
        assertEquals(0, stats.lastSuccessfulDownloadTimestamp);
    }

    // Validate if the shardRouting obtained from cluster state contains the exact same routing object
    // parameters as obtained from the remote store stats API
    private void validateShardRouting(ShardRouting routing) {
        Stream<ShardRouting> currentRoutingTable = getClusterState().routingTable()
            .getIndicesRouting()
            .get(INDEX_NAME)
            .shard(routing.id())
            .assignedShards()
            .stream();
        assertTrue(
            currentRoutingTable.anyMatch(
                r -> (r.currentNodeId().equals(routing.currentNodeId())
                    && r.state().equals(routing.state())
                    && r.primary() == routing.primary())
            )
        );
    }
}
