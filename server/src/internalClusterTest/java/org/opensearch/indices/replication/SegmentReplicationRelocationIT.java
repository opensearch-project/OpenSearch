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
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * This test class verifies primary shard relocation with segment replication as replication strategy.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationRelocationIT extends SegmentReplicationIT {
    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);

    private void createIndex() {
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put("index.number_of_replicas", 1)
        ).get();
    }

    /**
     * This test verifies happy path when primary shard is relocated newly added node (target) in the cluster. Before
     * relocation and after relocation documents are indexed and documents are verified
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/5669")
    public void testPrimaryRelocation() throws Exception {
        final String oldPrimary = internalCluster().startNode(featureFlagSettings());
        createIndex();
        final String replica = internalCluster().startNode(featureFlagSettings());
        ensureGreen(INDEX_NAME);
        final int initialDocCount = scaledRandomIntBetween(0, 200);
        ingestDocs(initialDocCount);

        logger.info("--> verifying count {}", initialDocCount);
        assertHitCount(client(oldPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

        logger.info("--> start another node");
        final String newPrimary = internalCluster().startNode(featureFlagSettings());
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

        logger.info("--> state {}", state);

        assertEquals(
            state.getRoutingNodes().node(state.nodes().resolveNode(newPrimary).getId()).iterator().next().state(),
            ShardRoutingState.STARTED
        );

        final int finalDocCount = initialDocCount;
        ingestDocs(finalDocCount);
        refresh(INDEX_NAME);

        logger.info("--> verifying count again {}", initialDocCount + finalDocCount);
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(
            client(newPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(),
            initialDocCount + finalDocCount
        );
        assertHitCount(
            client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(),
            initialDocCount + finalDocCount
        );
    }

    /**
     * This test verifies the primary relocation behavior when segment replication round fails during recovery. Post
     * failure, more documents are ingested and verified on replica; which confirms older primary still refreshing the
     * replicas.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/5669")
    public void testPrimaryRelocationWithSegRepFailure() throws Exception {
        final String oldPrimary = internalCluster().startNode(featureFlagSettings());
        createIndex();
        final String replica = internalCluster().startNode(featureFlagSettings());
        ensureGreen(INDEX_NAME);
        final int initialDocCount = scaledRandomIntBetween(1, 100);
        ingestDocs(initialDocCount);

        logger.info("--> verifying count {}", initialDocCount);
        assertHitCount(client(oldPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

        logger.info("--> start another node");
        final String newPrimary = internalCluster().startNode(featureFlagSettings());
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

        final int finalDocCount = initialDocCount;
        ingestDocs(finalDocCount);
        refresh(INDEX_NAME);

        logger.info("Verify older primary is still refreshing replica nodes");
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(
            client(oldPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(),
            initialDocCount + finalDocCount
        );
        assertHitCount(
            client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(),
            initialDocCount + finalDocCount
        );
    }

    /**
     * This test verifies primary recovery behavior with continuous ingestion
     *
     */
    public void testRelocateWhileContinuouslyIndexingAndWaitingForRefresh() throws Exception {
        final String primary = internalCluster().startNode(featureFlagSettings());
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", -1)
        ).get();

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

        final String replica = internalCluster().startNode(featureFlagSettings());
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        logger.info("--> relocate the shard from primary to replica");
        ActionFuture<ClusterRerouteResponse> relocationListener = client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, primary, replica))
            .execute();
        for (int i = 20; i < 120; i++) {
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
        assertEquals(client().prepareSearch(INDEX_NAME).setSize(0).execute().actionGet().getHits().getTotalHits().value, 120L);
    }
}
