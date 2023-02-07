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

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationApiIT extends SegmentReplicationBaseIT {

    public void testSegmentReplicationApiResponse() throws Exception {
        logger.info("--> starting [Primary Node] ...");
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        logger.info("--> start empty node to add replica shard");
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(10L, asList(primaryNode, replicaNode));

        SegmentReplicationStatsResponse response = client().admin().indices().prepareSegmentReplication(INDEX_NAME).execute().actionGet();
        // Verify API Response
        assertThat(response.shardSegmentReplicationStates().size(), equalTo(SHARD_COUNT));
        assertThat(response.shardSegmentReplicationStates().get(INDEX_NAME).get(0).getStage(), equalTo(SegmentReplicationState.Stage.DONE));
        assertThat(response.shardSegmentReplicationStates().get(INDEX_NAME).get(0).getIndex().recoveredFileCount(), greaterThan(0));
    }

    public void testSegmentReplicationApiResponseForActiveAndCompletedOnly() throws Exception {
        logger.info("--> starting [Primary Node] ...");
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        logger.info("--> starting [Replica Node] ...");
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);
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
        logger.info("--> verifying active_only by checking if current stage is GET_FILES STAGE");
        SegmentReplicationStatsResponse activeOnlyResponse = client().admin()
            .indices()
            .prepareSegmentReplication(INDEX_NAME)
            .setActiveOnly(true)
            .execute()
            .actionGet();
        assertThat(
            activeOnlyResponse.shardSegmentReplicationStates().get(INDEX_NAME).get(0).getStage(),
            equalTo(SegmentReplicationState.Stage.GET_FILES)
        );

        logger.info("--> verifying completed_only by checking if current stage is DONE");
        SegmentReplicationStatsResponse completedOnlyResponse = client().admin()
            .indices()
            .prepareSegmentReplication(INDEX_NAME)
            .setCompletedOnly(true)
            .execute()
            .actionGet();
        assertThat(completedOnlyResponse.shardSegmentReplicationStates().size(), equalTo(SHARD_COUNT));
        assertThat(
            completedOnlyResponse.shardSegmentReplicationStates().get(INDEX_NAME).get(0).getStage(),
            equalTo(SegmentReplicationState.Stage.DONE)
        );
        waitForAssertions.countDown();
    }

    public void testSegmentReplicationApiResponseOnDocumentReplicationIndex() {
        logger.info("--> starting [Primary Node] ...");
        final String primaryNode = internalCluster().startNode();
        prepareCreate(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)

        ).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        logger.info("--> start empty node to add replica shard");
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        refresh(INDEX_NAME);
        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> client().admin().indices().prepareSegmentReplication(INDEX_NAME).execute().actionGet()
        );
        // Verify exception message
        String expectedMessage = "Segment Replication is not enabled on Index: test-idx-1";
        assertEquals(expectedMessage, exception.getMessage());

    }

}
