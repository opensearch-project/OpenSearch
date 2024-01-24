/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * These tests simulate corruption cases during replication.  They are skipped on WindowsFS simulation where file renaming
 * can fail with an access denied IOException because deletion is not permitted.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("WindowsFS")
public class SegmentReplicationDisruptionIT extends SegmentReplicationBaseIT {
    @Before
    private void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    public void testSendCorruptBytesToReplica() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.refresh_interval", -1)
                .build()
        );
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        MockTransportService primaryTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean(false);
        primaryTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replicaNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK) && failed.getAndSet(true) == false) {
                    FileChunkRequest req = (FileChunkRequest) request;
                    TransportRequest corrupt = new FileChunkRequest(
                        req.recoveryId(),
                        ((FileChunkRequest) request).requestSeqNo(),
                        ((FileChunkRequest) request).shardId(),
                        ((FileChunkRequest) request).metadata(),
                        ((FileChunkRequest) request).position(),
                        new BytesArray("test"),
                        false,
                        0,
                        0L
                    );
                    connection.sendRequest(requestId, action, corrupt, options);
                    latch.countDown();
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            }
        );
        for (int i = 0; i < 100; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .get();
        }
        final long originalRecoveryTime = getRecoveryStopTime(replicaNode);
        assertNotEquals(originalRecoveryTime, 0);
        refreshWithNoWaitForReplicas(INDEX_NAME);
        latch.await();
        assertTrue(failed.get());
        waitForNewPeerRecovery(replicaNode, originalRecoveryTime);
        // reset checkIndex to ensure our original shard doesn't throw
        resetCheckIndexStatus();
        waitForSearchableDocs(100, primaryNode, replicaNode);
    }

    public void testWipeSegmentBetweenSyncs() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.refresh_interval", -1)
                .build()
        );
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .get();
        }
        refreshWithNoWaitForReplicas(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        final long originalRecoveryTime = getRecoveryStopTime(replicaNode);

        final IndexShard indexShard = getIndexShard(replicaNode, INDEX_NAME);
        waitForSearchableDocs(INDEX_NAME, 10, List.of(replicaNode));
        indexShard.store().directory().deleteFile("_0.si");

        for (int i = 11; i < 21; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .get();
        }
        refreshWithNoWaitForReplicas(INDEX_NAME);
        waitForNewPeerRecovery(replicaNode, originalRecoveryTime);
        resetCheckIndexStatus();
        waitForSearchableDocs(20, primaryNode, replicaNode);
    }

    private void waitForNewPeerRecovery(String replicaNode, long originalRecoveryTime) throws Exception {
        assertBusy(() -> {
            // assert we have a peer recovery after the original
            final long time = getRecoveryStopTime(replicaNode);
            assertNotEquals(time, 0);
            assertNotEquals(originalRecoveryTime, time);

        }, 1, TimeUnit.MINUTES);
    }

    private long getRecoveryStopTime(String nodeName) {
        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(INDEX_NAME).get();
        final List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(INDEX_NAME);
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getTargetNode().getName().equals(nodeName)) {
                return recoveryState.getTimer().stopTime();
            }
        }
        return 0L;
    }
}
