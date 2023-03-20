/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * This test class verifies primary shard relocation with segment replication as replication strategy.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationRelocationIT extends SegmentReplicationBaseIT {
    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);

    private void createIndex(int replicaCount) {
        prepareCreate(INDEX_NAME, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount)).get();
    }

    /**
     * This test verifies happy path when primary shard is relocated newly added node (target) in the cluster. Before
     * relocation and after relocation documents are indexed and documents are verified
     */
    public void testPrimaryRelocation() throws Exception {
        final String oldPrimary = internalCluster().startNode();
        createIndex(1);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        final int initialDocCount = scaledRandomIntBetween(100, 1000);
        final WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
        final List<ActionFuture<IndexResponse>> pendingIndexResponses = new ArrayList<>();
        for (int i = 0; i < initialDocCount; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(refreshPolicy)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }

        logger.info("--> start another node");
        final String newPrimary = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        logger.info("--> relocate the shard");
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, oldPrimary, newPrimary))
            .execute()
            .actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        logger.info("--> get the state, verify shard 1 primary moved from node1 to node2");
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertEquals(
            state.getRoutingNodes().node(state.nodes().resolveNode(newPrimary).getId()).iterator().next().state(),
            ShardRoutingState.STARTED
        );

        for (int i = initialDocCount; i < 2 * initialDocCount; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(refreshPolicy)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }
        assertBusy(() -> {
            client().admin().indices().prepareRefresh().execute().actionGet();
            assertTrue(pendingIndexResponses.stream().allMatch(ActionFuture::isDone));
        }, 1, TimeUnit.MINUTES);
        flushAndRefresh(INDEX_NAME);
        logger.info("--> verify count again {}", 2 * initialDocCount);
        waitForSearchableDocs(2 * initialDocCount, newPrimary, replica);
        verifyStoreContent();
    }

    /**
     * This test verifies the primary relocation behavior when segment replication round fails during recovery. Post
     * failure, more documents are ingested and verified on replica; which confirms older primary still refreshing the
     * replicas.
     */
    public void testPrimaryRelocationWithSegRepFailure() throws Exception {
        final String oldPrimary = internalCluster().startNode();
        createIndex(1);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        final int initialDocCount = scaledRandomIntBetween(100, 1000);
        final WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
        final List<ActionFuture<IndexResponse>> pendingIndexResponses = new ArrayList<>();
        for (int i = 0; i < initialDocCount; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(refreshPolicy)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }

        logger.info("--> start another node");
        final String newPrimary = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        // Mock transport service to add behaviour of throwing corruption exception during segment replication process.
        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            oldPrimary
        ));
        mockTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, newPrimary),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
                    throw new OpenSearchCorruptionException("expected");
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        logger.info("--> relocate the shard");
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, oldPrimary, newPrimary))
            .execute()
            .actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        for (int i = initialDocCount; i < 2 * initialDocCount; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(refreshPolicy)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }

        logger.info("Verify older primary is still refreshing replica nodes");
        assertBusy(() -> {
            client().admin().indices().prepareRefresh().execute().actionGet();
            assertTrue(pendingIndexResponses.stream().allMatch(ActionFuture::isDone));
        }, 1, TimeUnit.MINUTES);
        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(2 * initialDocCount, oldPrimary, replica);
        verifyStoreContent();
    }

    /**
     * This test verifies primary recovery behavior with continuous ingestion
     *
     */
    public void testRelocateWhileContinuouslyIndexingAndWaitingForRefresh() throws Exception {
        final String primary = internalCluster().startNode();
        createIndex(1);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        final int totalDocCount = 1000;
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush to have segments on disk");
        client().admin().indices().prepareFlush().execute().actionGet();

        logger.info("--> index more docs so there are ops in the transaction log");
        final List<ActionFuture<IndexResponse>> pendingIndexResponses = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }

        final String newPrimary = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        logger.info("--> relocate the shard from primary to replica");
        ActionFuture<ClusterRerouteResponse> relocationListener = client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, primary, newPrimary))
            .execute();
        for (int i = 20; i < totalDocCount; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }
        relocationListener.actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        logger.info("--> verifying count");
        assertBusy(() -> {
            client().admin().indices().prepareRefresh().execute().actionGet();
            assertTrue(pendingIndexResponses.stream().allMatch(ActionFuture::isDone));
        }, 1, TimeUnit.MINUTES);
        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(totalDocCount, newPrimary, replica);
        verifyStoreContent();
    }

    /**
     * This test verifies delayed operations during primary handoff are replayed and searchable. It does so by halting
     * segment replication which is performed while holding primary indexing permits which results in queuing of
     * operations during handoff. The test verifies all docs ingested are searchable on new primary.
     *
     */
    public void testRelocateWithQueuedOperationsDuringHandoff() throws Exception {
        final String primary = internalCluster().startNode();
        createIndex(1);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        final int totalDocCount = 2000;

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush to have segments on disk");
        client().admin().indices().prepareFlush().execute().actionGet();
        final WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());

        logger.info("--> index more docs so there are ops in the transaction log");
        final List<ActionFuture<IndexResponse>> pendingIndexResponses = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(refreshPolicy)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }
        final String newPrimary = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);
        ensureGreen(INDEX_NAME);

        // Get mock transport service from newPrimary, halt recovery during segment replication (during handoff) to allow indexing in
        // parallel.
        MockTransportService mockTargetTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            newPrimary
        ));
        CountDownLatch blockSegRepLatch = new CountDownLatch(1);
        CountDownLatch waitForIndexingLatch = new CountDownLatch(1);
        mockTargetTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, primary),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES)) {
                    blockSegRepLatch.countDown();
                    try {
                        waitForIndexingLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        Thread indexingThread = new Thread(() -> {
            // Wait for relocation to halt at SegRep. Ingest docs at that point.
            try {
                blockSegRepLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (int i = 20; i < totalDocCount; i++) {
                pendingIndexResponses.add(
                    client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute()
                );
            }
            waitForIndexingLatch.countDown();
        });

        logger.info("--> relocate the shard from primary to newPrimary");
        ActionFuture<ClusterRerouteResponse> relocationListener = client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, primary, newPrimary))
            .execute();

        // This thread first waits for recovery to halt during segment replication. After which it ingests data to ensure
        // documents are queued.
        indexingThread.start();
        indexingThread.join();
        relocationListener.actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        logger.info("--> verifying count");
        assertBusy(() -> {
            client().admin().indices().prepareRefresh().execute().actionGet();
            assertTrue(pendingIndexResponses.stream().allMatch(ActionFuture::isDone));
        }, 2, TimeUnit.MINUTES);
        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(totalDocCount, replica, newPrimary);
        verifyStoreContent();
    }

    /**
     * This test verifies that adding a new node which results in peer recovery as replica; also bring replica's
     * replication checkpoint upto the primary's by performing a round of segment replication.
     */
    public void testNewlyAddedReplicaIsUpdated() throws Exception {
        final String primary = internalCluster().startNode();
        prepareCreate(INDEX_NAME, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
            .get();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush so we have some segment files on disk");
        flush(INDEX_NAME);
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);
        assertEquals(client().prepareSearch(INDEX_NAME).setSize(0).execute().actionGet().getHits().getTotalHits().value, 20L);

        logger.info("--> start empty node to add replica shard");
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        // Update replica count settings to 1 so that peer recovery triggers and recover replica
        assertAcked(
            client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen(INDEX_NAME);
        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(20, primary, replica);
        verifyStoreContent();
    }

    /**
     * This test verifies that replica shard is not added to the cluster when doing a round of segment replication fails during peer recovery.
     */
    public void testAddNewReplicaFailure() throws Exception {
        logger.info("--> starting [Primary Node] ...");
        final String primaryNode = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)

        ).get();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush so we have some segment files on disk");
        flush(INDEX_NAME);
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);
        logger.info("--> verifying count");
        assertEquals(client().prepareSearch(INDEX_NAME).setSize(0).execute().actionGet().getHits().getTotalHits().value, 20L);

        logger.info("--> start empty node to add replica shard");
        final String replica = internalCluster().startNode();

        final CountDownLatch waitForRecovery = new CountDownLatch(1);
        // Mock transport service to add behaviour of throwing corruption exception during segment replication process.
        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));
        mockTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replica),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
                    waitForRecovery.countDown();
                    throw new OpenSearchCorruptionException("expected");
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        ensureGreen(INDEX_NAME);
        // Add Replica shard to the new empty replica node
        assertAcked(
            client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 1))
        );
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, replica);
        waitForRecovery.await();
        assertBusy(() -> assertTrue(indicesService.hasIndex(resolveIndex(INDEX_NAME))));

        // Verify that cluster state is not green and replica shard failed during a round of segment replication is not added to the cluster
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .setWaitForGreenStatus()
            .setTimeout(TimeValue.timeValueSeconds(2))
            .execute()
            .actionGet();
        assertTrue(clusterHealthResponse.isTimedOut());
        ensureYellow(INDEX_NAME);
    }

    public void testFlushAfterRelocation() throws Exception {
        // Starting two nodes with primary and replica shards respectively.
        final String primaryNode = internalCluster().startNode();
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to control refreshes
                .put("index.refresh_interval", -1)
        ).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        // Start another empty node for relocation
        final String newPrimary = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);
        ensureGreen(INDEX_NAME);

        // Start indexing docs
        final int initialDocCount = scaledRandomIntBetween(2000, 3000);
        for (int i = 0; i < initialDocCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        // Verify segment replication event never happened on replica shard
        SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .execute()
            .actionGet();
        assertTrue(segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME).get(0).getReplicaStats().isEmpty());

        // Relocate primary to new primary. When new primary starts it does perform a flush.
        logger.info("--> relocate the shard from primary to newPrimary");
        ActionFuture<ClusterRerouteResponse> relocationListener = client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, primaryNode, newPrimary))
            .execute();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        // Verify if all docs are present in replica after relocation, if new relocated primary doesn't flush after relocation the below
        // assert will fail.
        assertBusy(() -> {
            assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setPreference("_only_local").setSize(0).get(), initialDocCount);
        });
    }
}
