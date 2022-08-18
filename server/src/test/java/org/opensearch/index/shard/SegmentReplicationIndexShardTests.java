/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SegmentReplicationIndexShardTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    /**
     *  Test that latestReplicationCheckpoint returns null only for docrep enabled indices
     */
    public void testReplicationCheckpointNullForDocRep() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, "DOCUMENT").put(Settings.EMPTY).build();
        final IndexShard indexShard = newStartedShard(false, indexSettings);
        assertNull(indexShard.getLatestReplicationCheckpoint());
        closeShards(indexShard);
    }

    /**
     *  Test that latestReplicationCheckpoint returns ReplicationCheckpoint for segrep enabled indices
     */
    public void testReplicationCheckpointNotNullForSegReb() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT").put(Settings.EMPTY).build();
        final IndexShard indexShard = newStartedShard(indexSettings);
        final ReplicationCheckpoint replicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
        assertNotNull(replicationCheckpoint);
        closeShards(indexShard);
    }

    public void testSegmentReplication_Index_Update_Delete() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(2, settings, mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();

            final int numDocs = randomIntBetween(100, 200);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }

            primaryShard.refresh("Test");
            replicateSegments(primaryShard, shards.getReplicas());

            shards.assertAllEqual(numDocs);

            for (int i = 0; i < numDocs; i++) {
                // randomly update docs.
                if (randomBoolean()) {
                    shards.index(
                        new IndexRequest(index.getName()).id(String.valueOf(i)).source("{ \"foo\" : \"baz\" }", XContentType.JSON)
                    );
                }
            }

            primaryShard.refresh("Test");
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            final List<DocIdSeqNoAndSource> docs = getDocIdAndSeqNos(primaryShard);
            for (IndexShard shard : shards.getReplicas()) {
                assertEquals(getDocIdAndSeqNos(shard), docs);
            }
            for (int i = 0; i < numDocs; i++) {
                // randomly delete.
                if (randomBoolean()) {
                    shards.delete(new DeleteRequest(index.getName()).id(String.valueOf(i)));
                }
            }
            primaryShard.refresh("Test");
            replicateSegments(primaryShard, shards.getReplicas());
            final List<DocIdSeqNoAndSource> docsAfterDelete = getDocIdAndSeqNos(primaryShard);
            for (IndexShard shard : shards.getReplicas()) {
                assertEquals(getDocIdAndSeqNos(shard), docsAfterDelete);
            }
        }
    }

    public void testIgnoreShardIdle() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            primary.scheduledRefresh();
            replica.scheduledRefresh();

            primary.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));
            replica.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));

            // Update the search_idle setting, this will put both shards into search idle.
            Settings updatedSettings = Settings.builder()
                .put(settings)
                .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
                .build();
            primary.indexSettings().getScopedSettings().applySettings(updatedSettings);
            replica.indexSettings().getScopedSettings().applySettings(updatedSettings);

            primary.scheduledRefresh();
            replica.scheduledRefresh();

            // Shards without segrep will register a new RefreshListener on the engine and return true when registered,
            // assert with segrep enabled that awaitShardSearchActive does not register a listener.
            primary.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));
            replica.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));
        }
    }

    /**
     * here we are starting a new primary shard in PrimaryMode and testing if the shard publishes checkpoint after refresh.
     */
    public void testPublishCheckpointOnPrimaryMode() throws IOException {
        final SegmentReplicationCheckpointPublisher mock = mock(SegmentReplicationCheckpointPublisher.class);
        IndexShard shard = newStartedShard(true);
        CheckpointRefreshListener refreshListener = new CheckpointRefreshListener(shard, mock);
        refreshListener.afterRefresh(true);

        // verify checkpoint is published
        verify(mock, times(1)).publish(any());
        closeShards(shard);
    }

    /**
     * here we are starting a new primary shard in PrimaryMode initially and starting relocation handoff. Later we complete relocation handoff then shard is no longer
     * in PrimaryMode, and we test if the shard does not publish checkpoint after refresh.
     */
    public void testPublishCheckpointAfterRelocationHandOff() throws IOException {
        final SegmentReplicationCheckpointPublisher mock = mock(SegmentReplicationCheckpointPublisher.class);
        IndexShard shard = newStartedShard(true);
        CheckpointRefreshListener refreshListener = new CheckpointRefreshListener(shard, mock);
        String id = shard.routingEntry().allocationId().getId();

        // Starting relocation handoff
        shard.getReplicationTracker().startRelocationHandoff(id);

        // Completing relocation handoff
        shard.getReplicationTracker().completeRelocationHandoff();
        refreshListener.afterRefresh(true);

        // verify checkpoint is not published
        verify(mock, times(0)).publish(any());
        closeShards(shard);
    }

    public void testNRTReplicaPromotedAsPrimary() throws Exception {
        try (ReplicationGroup shards = createGroup(2, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();
            final IndexShard nextPrimary = shards.getReplicas().get(0);
            final IndexShard replica = shards.getReplicas().get(1);

            // 1. Create ops that are in the index and xlog of both shards but not yet part of a commit point.
            final int numDocs = shards.indexDocs(randomInt(10));

            // refresh and copy the segments over.
            oldPrimary.refresh("Test");
            replicateSegments(oldPrimary, shards.getReplicas());

            // at this point both shards should have numDocs persisted and searchable.
            assertDocCounts(oldPrimary, numDocs, numDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, numDocs, numDocs);
            }

            // 2. Create ops that are in the replica's xlog, not in the index.
            // index some more into both but don't replicate. replica will have only numDocs searchable, but should have totalDocs
            // persisted.
            final int totalDocs = numDocs + shards.indexDocs(randomInt(10));

            assertDocCounts(oldPrimary, totalDocs, totalDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, totalDocs, numDocs);
            }

            // promote the replica
            shards.syncGlobalCheckpoint();
            assertEquals(totalDocs, nextPrimary.translogStats().estimatedNumberOfOperations());
            shards.promoteReplicaToPrimary(nextPrimary);

            // close and start the oldPrimary as a replica.
            oldPrimary.close("demoted", false);
            oldPrimary.store().close();
            oldPrimary = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(oldPrimary);

            assertEquals(NRTReplicationEngine.class, oldPrimary.getEngine().getClass());
            assertEquals(InternalEngine.class, nextPrimary.getEngine().getClass());
            assertDocCounts(nextPrimary, totalDocs, totalDocs);
            assertEquals(0, nextPrimary.translogStats().estimatedNumberOfOperations());

            // refresh and push segments to our other replica.
            nextPrimary.refresh("test");
            replicateSegments(nextPrimary, asList(replica));

            for (IndexShard shard : shards) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final List<DocIdSeqNoAndSource> docsAfterRecovery = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
            }
        }
    }

    /**
     * Assert persisted and searchable doc counts.  This method should not be used while docs are concurrently indexed because
     * it asserts point in time seqNos are relative to the doc counts.
     */
    private void assertDocCounts(IndexShard indexShard, int expectedPersistedDocCount, int expectedSearchableDocCount) throws IOException {
        assertDocCount(indexShard, expectedSearchableDocCount);
        // assigned seqNos start at 0, so assert max & local seqNos are 1 less than our persisted doc count.
        assertEquals(expectedPersistedDocCount - 1, indexShard.seqNoStats().getMaxSeqNo());
        assertEquals(expectedPersistedDocCount - 1, indexShard.seqNoStats().getLocalCheckpoint());
        // processed cp should be 1 less than our searchable doc count.
        assertEquals(expectedSearchableDocCount - 1, indexShard.getProcessedLocalCheckpoint());
    }
}
