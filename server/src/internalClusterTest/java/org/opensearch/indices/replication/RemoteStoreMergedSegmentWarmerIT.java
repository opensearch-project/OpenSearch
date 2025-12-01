/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.RemoteStorePublishMergedSegmentRequest;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMergedSegmentWarmerIT extends SegmentReplicationBaseIT {
    private Path absolutePath;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings("test-remote-store-repo", absolutePath))
            .put(RecoverySettings.INDICES_MERGED_SEGMENT_REPLICATION_WARMER_ENABLED_SETTING.getKey(), true)
            .put(RecoverySettings.INDICES_REPLICATION_MERGES_WARMER_MIN_SEGMENT_SIZE_THRESHOLD_SETTING.getKey(), "1b")
            .build();
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    public void testMergeSegmentWarmerRemote() throws Exception {
        final String node1 = internalCluster().startDataOnlyNode();
        final String node2 = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        String primaryShardNode = findprimaryShardNode(INDEX_NAME);
        MockTransportService mockTransportServicePrimary = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryShardNode
        );
        final CountDownLatch latch = new CountDownLatch(1);
        StubbableTransport.SendRequestBehavior behavior = (connection, requestId, action, request, options) -> {
            if (action.equals("indices:admin/remote_publish_merged_segment[r]")) {
                assertTrue(
                    ((TransportReplicationAction.ConcreteReplicaRequest) request)
                        .getRequest() instanceof RemoteStorePublishMergedSegmentRequest
                );
                latch.countDown();
            }
            connection.sendRequest(requestId, action, request, options);
        };

        mockTransportServicePrimary.addSendBehavior(behavior);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        waitForSearchableDocs(30, node1, node2);

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(2));
        waitForSegmentCount(INDEX_NAME, 2, logger);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        mockTransportServicePrimary.clearAllRules();
    }

    public void testConcurrentMergeSegmentWarmerRemote() throws Exception {
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 5)
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 5)
                .put(IndexSettings.INDEX_MERGE_ON_FLUSH_ENABLED.getKey(), false)
                .build()
        );
        ensureGreen(INDEX_NAME);

        String primaryShardNode = findprimaryShardNode(INDEX_NAME);
        MockTransportService mockTransportServicePrimary = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryShardNode
        );

        CountDownLatch latch = new CountDownLatch(2);
        AtomicLong numInvocations = new AtomicLong(0);
        Set<String> executingThreads = ConcurrentHashMap.newKeySet();
        StubbableTransport.SendRequestBehavior behavior = (connection, requestId, action, request, options) -> {
            if (action.equals("indices:admin/remote_publish_merged_segment[r]")) {
                assertTrue(
                    ((TransportReplicationAction.ConcreteReplicaRequest) request)
                        .getRequest() instanceof RemoteStorePublishMergedSegmentRequest
                );
                latch.countDown();
                numInvocations.incrementAndGet();
                executingThreads.add(Thread.currentThread().getName());
            }
            connection.sendRequest(requestId, action, request, options);
        };

        mockTransportServicePrimary.addSendBehavior(behavior);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(2));

        waitForSegmentCount(INDEX_NAME, 2, logger);
        logger.info("Number of merge invocations: {}", numInvocations.get());
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(executingThreads.size() > 1);
        // Verify concurrent execution by checking that multiple unique threads handled merge operations
        assertTrue(numInvocations.get() > 1);
        mockTransportServicePrimary.clearAllRules();
    }

    public void testMergeSegmentWarmerWithInactiveReplicaRemote() throws Exception {
        internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1)).get();
        final IndicesSegmentResponse response = client().admin().indices().prepareSegments(INDEX_NAME).get();
        assertEquals(1, response.getIndices().get(INDEX_NAME).getShards().values().size());
    }

    public void testMergeSegmentWarmerWithWarmingDisabled() throws Exception {
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        String primaryNodeName = findprimaryShardNode(INDEX_NAME);
        internalCluster().client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(RecoverySettings.INDICES_MERGED_SEGMENT_REPLICATION_WARMER_ENABLED_SETTING.getKey(), false).build()
            )
            .get();

        MockTransportService mockTransportServicePrimary = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNodeName
        );

        CountDownLatch warmingLatch = new CountDownLatch(1);
        StubbableTransport.SendRequestBehavior behavior = (connection, requestId, action, request, options) -> {
            if (action.equals("indices:admin/remote_publish_merged_segment[r]")) {
                warmingLatch.countDown(); // This should NOT happen
            }
            connection.sendRequest(requestId, action, request, options);
        };

        mockTransportServicePrimary.addSendBehavior(behavior);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1)).get();
        final IndicesSegmentResponse response = client().admin().indices().prepareSegments(INDEX_NAME).get();
        assertEquals(1, response.getIndices().get(INDEX_NAME).getShards().values().size());
        assertFalse("Warming should be skipped when disabled", warmingLatch.await(5, TimeUnit.SECONDS));
        mockTransportServicePrimary.clearAllRules();
    }

    /**
     * Returns the node name for the node hosting the primary shard for index "indexName"
     */
    private String findprimaryShardNode(String indexName) {
        String nodeId = internalCluster().clusterService().state().routingTable().index(indexName).shard(0).primaryShard().currentNodeId();

        return internalCluster().clusterService().state().nodes().get(nodeId).getName();

    }
}
