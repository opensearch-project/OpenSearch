/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationStatsIT extends SegmentReplicationBaseIT {

    public void testSegmentReplicationStatsResponse() {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String anotherDataNode = internalCluster().startDataOnlyNode();

        int numShards = 4;
        assertAcked(
            prepareCreate(
                INDEX_NAME,
                0,
                Settings.builder()
                    .put("number_of_shards", numShards)
                    .put("number_of_replicas", 1)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            )
        );
        ensureGreen();
        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index(INDEX_NAME, "doc", Integer.toString(i));
        }
        refresh(INDEX_NAME);
        ensureSearchable(INDEX_NAME);

        SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .execute()
            .actionGet();
        assertThat(segmentReplicationStatsResponse.shardSegmentReplicationStates().size(), equalTo(1));
        assertThat(segmentReplicationStatsResponse.getTotalShards(), equalTo(numShards * 2));
        assertThat(segmentReplicationStatsResponse.getSuccessfulShards(), equalTo(numShards * 2));
    }

    public void testSegmentReplicationStatsResponseForActiveAndCompletedOnly() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        // index 10 docs
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);

        // index 10 more docs
        waitForSearchableDocs(10L, asList(primaryNode, replicaNode));
        for (int i = 10; i < 20; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        final CountDownLatch waitForReplication = new CountDownLatch(1);

        final CountDownLatch waitForAssertions = new CountDownLatch(1);
        // Mock transport service to add behaviour of waiting in GET_SEGMENT_FILES Stage of a segment replication event.
        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            replicaNode
        ));
        mockTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, primaryNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES)) {
                    waitForReplication.countDown();
                    try {
                        waitForAssertions.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        refresh(INDEX_NAME);
        try {
            waitForReplication.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // verifying active_only by checking if current stage is GET_FILES STAGE
        SegmentReplicationStatsResponse activeOnlyResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setActiveOnly(true)
            .execute()
            .actionGet();
        assertEquals(
            activeOnlyResponse.shardSegmentReplicationStates().get(INDEX_NAME).get(0).getStage(),
            SegmentReplicationState.Stage.GET_FILES
        );

        // verifying completed_only by checking if current stage is DONE
        SegmentReplicationStatsResponse completedOnlyResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setCompletedOnly(true)
            .execute()
            .actionGet();
        assertEquals(completedOnlyResponse.shardSegmentReplicationStates().size(), SHARD_COUNT);
        assertEquals(
            completedOnlyResponse.shardSegmentReplicationStates().get(INDEX_NAME).get(0).getStage(),
            SegmentReplicationState.Stage.DONE
        );
        assertThat(
            completedOnlyResponse.shardSegmentReplicationStates().get(INDEX_NAME).get(0).getIndex().recoveredFileCount(),
            greaterThan(0)
        );
        waitForAssertions.countDown();
    }

    public void testSegmentReplicationStatsResponseOnDocumentReplicationIndex() {
        final String primaryNode = internalCluster().startNode();
        prepareCreate(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)

        ).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        // index 10 docs
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);
        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> client().admin().indices().prepareSegmentReplicationStats(INDEX_NAME).execute().actionGet()
        );
        // Verify exception message
        String expectedMessage = "Segment Replication is not enabled on Index: test-idx-1";
        assertEquals(expectedMessage, exception.getMessage());

    }

}
