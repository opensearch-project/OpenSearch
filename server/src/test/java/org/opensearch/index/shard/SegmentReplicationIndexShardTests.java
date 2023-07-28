/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.junit.Assert;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.SnapshotMatchers;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentReplicationIndexShardTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    /**
     * Test that latestReplicationCheckpoint returns null only for docrep enabled indices
     */
    public void testReplicationCheckpointNullForDocRep() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, "DOCUMENT").put(Settings.EMPTY).build();
        final IndexShard indexShard = newStartedShard(false, indexSettings);
        assertNull(indexShard.getLatestReplicationCheckpoint());
        closeShards(indexShard);
    }

    /**
     * Test that latestReplicationCheckpoint returns ReplicationCheckpoint for segrep enabled indices
     */
    public void testReplicationCheckpointNotNullForSegRep() throws IOException {
        final IndexShard indexShard = newStartedShard(randomBoolean(), settings, new NRTReplicationEngineFactory());
        final ReplicationCheckpoint replicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
        assertNotNull(replicationCheckpoint);
        closeShards(indexShard);
    }

    public void testNRTReplicasDoNotAcceptRefreshListeners() throws IOException {
        final IndexShard indexShard = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        indexShard.addRefreshListener(mock(Translog.Location.class), Assert::assertFalse);
        closeShards(indexShard);
    }

    public void testSegmentInfosAndReplicationCheckpointTuple() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            // assert before any indexing:
            // replica:
            Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> replicaTuple = replica.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = replicaTuple.v1()) {
                assertReplicationCheckpoint(replica, gatedCloseable.get(), replicaTuple.v2());
            }

            // primary:
            Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> primaryTuple = primary.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = primaryTuple.v1()) {
                assertReplicationCheckpoint(primary, gatedCloseable.get(), primaryTuple.v2());
            }
            // We use compareTo here instead of equals because we ignore segments gen with replicas performing their own commits.
            // However infos version we expect to be equal.
            assertEquals(1, primary.getLatestReplicationCheckpoint().compareTo(replica.getLatestReplicationCheckpoint()));

            // index and copy segments to replica.
            int numDocs = randomIntBetween(10, 100);
            shards.indexDocs(numDocs);
            primary.refresh("test");
            replicateSegments(primary, List.of(replica));

            replicaTuple = replica.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = replicaTuple.v1()) {
                assertReplicationCheckpoint(replica, gatedCloseable.get(), replicaTuple.v2());
            }

            primaryTuple = primary.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = primaryTuple.v1()) {
                assertReplicationCheckpoint(primary, gatedCloseable.get(), primaryTuple.v2());
            }

            replicaTuple = replica.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = replicaTuple.v1()) {
                assertReplicationCheckpoint(replica, gatedCloseable.get(), replicaTuple.v2());
            }
            assertEquals(1, primary.getLatestReplicationCheckpoint().compareTo(replica.getLatestReplicationCheckpoint()));
        }
    }

    private void assertReplicationCheckpoint(IndexShard shard, SegmentInfos segmentInfos, ReplicationCheckpoint checkpoint)
        throws IOException {
        assertNotNull(segmentInfos);
        assertEquals(checkpoint.getSegmentInfosVersion(), segmentInfos.getVersion());
        assertEquals(checkpoint.getSegmentsGen(), segmentInfos.getGeneration());
    }

    public void testIsSegmentReplicationAllowed_WrongEngineType() throws IOException {
        final IndexShard indexShard = newShard(false, settings, new InternalEngineFactory());
        assertFalse(indexShard.isSegmentReplicationAllowed());
        closeShards(indexShard);
    }

    /**
     * This test mimics the segment replication failure due to CorruptIndexException exception which happens when
     * reader close operation on replica shard deletes the segment files copied in current round of segment replication.
     * It does this by blocking the finalizeReplication on replica shard and performing close operation on acquired
     * searcher that triggers the reader close operation.
     */
    public void testSegmentReplication_With_ReaderClosedConcurrently() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(1, settings, mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);

            // Step 1. Ingest numDocs documents & replicate to replica shard
            final int numDocs = randomIntBetween(100, 200);
            logger.info("--> Inserting documents {}", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());

            IndexShard spyShard = spy(replicaShard);
            Engine.Searcher test = replicaShard.getEngine().acquireSearcher("testSegmentReplication_With_ReaderClosedConcurrently");
            shards.assertAllEqual(numDocs);

            // Step 2. Ingest numDocs documents again & replicate to replica shard
            logger.info("--> Ingest {} docs again", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());

            // Step 3. Perform force merge down to 1 segment on primary
            primaryShard.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(true));
            logger.info("--> primary store after force merge {}", Arrays.toString(primaryShard.store().directory().listAll()));
            // Perform close on searcher before IndexShard::finalizeReplication
            doAnswer(n -> {
                test.close();
                n.callRealMethod();
                return null;
            }).when(spyShard).finalizeReplication(any());
            replicateSegments(primaryShard, List.of(spyShard));
            shards.assertAllEqual(numDocs);
        }
    }

    /**
     * Similar to test above, this test shows the issue where an engine close operation during active segment replication
     * can result in Lucene CorruptIndexException.
     */
    public void testSegmentReplication_With_EngineClosedConcurrently() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(1, settings, mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);

            // Step 1. Ingest numDocs documents
            final int numDocs = randomIntBetween(100, 200);
            logger.info("--> Inserting documents {}", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            // Step 2. Ingest numDocs documents again to create a new commit
            logger.info("--> Ingest {} docs again", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            logger.info("--> primary store after final flush {}", Arrays.toString(primaryShard.store().directory().listAll()));

            // Step 3. Before replicating segments, block finalizeReplication and perform engine commit directly that
            // cleans up recently copied over files
            IndexShard spyShard = spy(replicaShard);
            doAnswer(n -> {
                NRTReplicationEngine engine = (NRTReplicationEngine) replicaShard.getEngine();
                // Using engine.close() prevents indexShard.finalizeReplication execution due to engine AlreadyClosedException,
                // thus as workaround, use updateSegments which eventually calls commitSegmentInfos on latest segment infos.
                engine.updateSegments(engine.getSegmentInfosSnapshot().get());
                n.callRealMethod();
                return null;
            }).when(spyShard).finalizeReplication(any());
            replicateSegments(primaryShard, List.of(spyShard));
            shards.assertAllEqual(numDocs);
        }
    }

    /**
     * Verifies that commits on replica engine resulting from engine or reader close does not cleanup the temporary
     * replication files from ongoing round of segment replication
     */
    public void testTemporaryFilesNotCleanup() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(1, settings, mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primaryShard = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            // Step 1. Ingest numDocs documents, commit to create commit point on primary & replicate
            final int numDocs = randomIntBetween(100, 200);
            logger.info("--> Inserting documents {}", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            // Step 2. Ingest numDocs documents again to create a new commit on primary
            logger.info("--> Ingest {} docs again", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));

            // Step 3. Copy segment files to replica shard but prevent commit
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            Map<String, StoreFileMetadata> primaryMetadata;
            try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = primaryShard.getSegmentInfosSnapshot()) {
                final SegmentInfos primarySegmentInfos = segmentInfosSnapshot.get();
                primaryMetadata = primaryShard.store().getSegmentMetadataMap(primarySegmentInfos);
            }
            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final IndicesService indicesService = mock(IndicesService.class);
            when(indicesService.getShardOrNull(replica.shardId)).thenReturn(replica);
            final SegmentReplicationTargetService targetService = new SegmentReplicationTargetService(
                threadPool,
                new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                mock(TransportService.class),
                sourceFactory,
                indicesService,
                clusterService
            );
            final Consumer<IndexShard> runnablePostGetFiles = (indexShard) -> {
                try {
                    Collection<String> temporaryFiles = Stream.of(indexShard.store().directory().listAll())
                        .filter(name -> name.startsWith(SegmentReplicationTarget.REPLICATION_PREFIX))
                        .collect(Collectors.toList());

                    // Step 4. Perform a commit on replica shard.
                    NRTReplicationEngine engine = (NRTReplicationEngine) indexShard.getEngine();
                    engine.updateSegments(engine.getSegmentInfosSnapshot().get());

                    // Step 5. Validate temporary files are not deleted from store.
                    Collection<String> replicaStoreFiles = List.of(indexShard.store().directory().listAll());
                    assertTrue(replicaStoreFiles.containsAll(temporaryFiles));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            SegmentReplicationSource segmentReplicationSource = getSegmentReplicationSource(
                primaryShard,
                (repId) -> targetService.get(repId),
                runnablePostGetFiles
            );
            when(sourceFactory.get(any())).thenReturn(segmentReplicationSource);
            targetService.startReplication(replica, getTargetListener(primaryShard, replica, primaryMetadata, countDownLatch));
            countDownLatch.await(30, TimeUnit.SECONDS);
            assertEquals("Replication failed", 0, countDownLatch.getCount());
            shards.assertAllEqual(numDocs);
        }
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

            assertEqualTranslogOperations(shards, primaryShard);
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
            assertEqualTranslogOperations(shards, primaryShard);
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
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            replicateSegments(primaryShard, shards.getReplicas());
            final List<DocIdSeqNoAndSource> docsAfterDelete = getDocIdAndSeqNos(primaryShard);
            for (IndexShard shard : shards.getReplicas()) {
                assertEquals(getDocIdAndSeqNos(shard), docsAfterDelete);
            }
        }
    }

    public void testIgnoreShardIdle() throws Exception {
        Settings updatedSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
            .build();
        try (ReplicationGroup shards = createGroup(1, updatedSettings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final int numDocs = shards.indexDocs(randomIntBetween(1, 10));
            // ensure search idle conditions are met.
            assertTrue(primary.isSearchIdle());
            assertTrue(replica.isSearchIdle());

            // invoke scheduledRefresh, returns true if refresh is immediately invoked.
            assertTrue(primary.scheduledRefresh());
            // replica would always return false here as there is no indexed doc to refresh on.
            assertFalse(replica.scheduledRefresh());

            // assert there is no pending refresh
            assertFalse(primary.hasRefreshPending());
            assertFalse(replica.hasRefreshPending());
            shards.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs);
        }
    }

    public void testShardIdle_Docrep() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build();
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final int numDocs = shards.indexDocs(randomIntBetween(1, 10));
            // ensure search idle conditions are met.
            assertTrue(primary.isSearchIdle());
            assertTrue(replica.isSearchIdle());
            assertFalse(primary.scheduledRefresh());
            assertFalse(replica.scheduledRefresh());
            assertTrue(primary.hasRefreshPending());
            assertTrue(replica.hasRefreshPending());
            shards.refresh("test");
            shards.assertAllEqual(numDocs);
        }
    }

    public void testShardIdleWithNoReplicas() throws Exception {
        Settings updatedSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
            .build();
        try (ReplicationGroup shards = createGroup(0, updatedSettings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            shards.indexDocs(randomIntBetween(1, 10));
            // ensure search idle conditions are met.
            assertTrue(primary.isSearchIdle());
            assertFalse(primary.scheduledRefresh());
            assertTrue(primary.hasRefreshPending());
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
        verify(mock, times(1)).publish(any(), any());
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
        verify(mock, times(0)).publish(any(), any());
        closeShards(shard);
    }

    /**
     * here we are starting a new primary shard and testing that we don't process a checkpoint on a shard when it's shard routing is primary.
     */
    public void testRejectCheckpointOnShardRoutingPrimary() throws IOException {
        IndexShard primaryShard = newStartedShard(true);
        SegmentReplicationTargetService sut;
        sut = prepareForReplication(primaryShard, null);
        SegmentReplicationTargetService spy = spy(sut);

        // Starting a new shard in PrimaryMode and shard routing primary.
        IndexShard spyShard = spy(primaryShard);
        String id = primaryShard.routingEntry().allocationId().getId();

        // Starting relocation handoff
        primaryShard.getReplicationTracker().startRelocationHandoff(id);

        // Completing relocation handoff.
        primaryShard.getReplicationTracker().completeRelocationHandoff();

        // Assert that primary shard is no longer in Primary Mode and shard routing is still Primary
        assertEquals(false, primaryShard.getReplicationTracker().isPrimaryMode());
        assertEquals(true, primaryShard.routingEntry().primary());

        spy.onNewCheckpoint(new ReplicationCheckpoint(primaryShard.shardId(), 0L, 0L, 0L, Codec.getDefault().getName()), spyShard);

        // Verify that checkpoint is not processed as shard routing is primary.
        verify(spy, times(0)).startReplication(any(), any());
        closeShards(primaryShard);
    }

    public void testReplicaReceivesGenIncrease() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final int numDocs = randomIntBetween(10, 100);
            shards.indexDocs(numDocs);
            assertEquals(numDocs, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(numDocs, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(numDocs, primary.translogStats().getUncommittedOperations());
            assertEquals(numDocs, replica.translogStats().getUncommittedOperations());
            flushShard(primary, true);
            replicateSegments(primary, shards.getReplicas());
            assertEquals(0, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(0, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(0, primary.translogStats().getUncommittedOperations());
            assertEquals(0, replica.translogStats().getUncommittedOperations());

            final int additionalDocs = shards.indexDocs(randomIntBetween(numDocs + 1, numDocs + 10));

            final int totalDocs = numDocs + additionalDocs;
            primary.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            assertEquals(additionalDocs, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(additionalDocs, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(additionalDocs, primary.translogStats().getUncommittedOperations());
            assertEquals(additionalDocs, replica.translogStats().getUncommittedOperations());
            flushShard(primary, true);
            replicateSegments(primary, shards.getReplicas());

            assertEqualCommittedSegments(primary, replica);
            assertDocCount(primary, totalDocs);
            assertDocCount(replica, totalDocs);
            assertEquals(0, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(0, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(0, primary.translogStats().getUncommittedOperations());
            assertEquals(0, replica.translogStats().getUncommittedOperations());
        }
    }

    public void testPrimaryRelocation() throws Exception {
        final IndexShard primarySource = newStartedShard(true, settings);
        int totalOps = randomInt(10);
        for (int i = 0; i < totalOps; i++) {
            indexDoc(primarySource, "_doc", Integer.toString(i));
        }
        IndexShardTestCase.updateRoutingEntry(primarySource, primarySource.routingEntry().relocate(randomAlphaOfLength(10), -1));
        final IndexShard primaryTarget = newShard(
            primarySource.routingEntry().getTargetRelocatingShard(),
            settings,
            new NRTReplicationEngineFactory()
        );
        updateMappings(primaryTarget, primarySource.indexSettings().getIndexMetadata());

        Function<List<IndexShard>, List<SegmentReplicationTarget>> replicatePrimaryFunction = (shardList) -> {
            try {
                assert shardList.size() >= 2;
                final IndexShard primary = shardList.get(0);
                return replicateSegments(primary, shardList.subList(1, shardList.size()));
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        recoverReplica(primaryTarget, primarySource, true, replicatePrimaryFunction);

        // check that local checkpoint of new primary is properly tracked after primary relocation
        assertThat(primaryTarget.getLocalCheckpoint(), equalTo(totalOps - 1L));
        assertThat(
            primaryTarget.getReplicationTracker()
                .getTrackedLocalCheckpointForShard(primaryTarget.routingEntry().allocationId().getId())
                .getLocalCheckpoint(),
            equalTo(totalOps - 1L)
        );
        assertDocCount(primaryTarget, totalOps);
        closeShards(primarySource, primaryTarget);
    }

    public void testPrimaryRelocationWithSegRepFailure() throws Exception {
        final IndexShard primarySource = newStartedShard(true, settings);
        int totalOps = randomInt(10);
        for (int i = 0; i < totalOps; i++) {
            indexDoc(primarySource, "_doc", Integer.toString(i));
        }
        IndexShardTestCase.updateRoutingEntry(primarySource, primarySource.routingEntry().relocate(randomAlphaOfLength(10), -1));
        final IndexShard primaryTarget = newShard(
            primarySource.routingEntry().getTargetRelocatingShard(),
            settings,
            new NRTReplicationEngineFactory()
        );
        updateMappings(primaryTarget, primarySource.indexSettings().getIndexMetadata());

        Function<List<IndexShard>, List<SegmentReplicationTarget>> replicatePrimaryFunction = (shardList) -> {
            try {
                throw new IOException("Expected failure");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        Exception e = expectThrows(
            Exception.class,
            () -> recoverReplica(
                primaryTarget,
                primarySource,
                (primary, sourceNode) -> new RecoveryTarget(primary, sourceNode, new ReplicationListener() {
                    @Override
                    public void onDone(ReplicationState state) {
                        throw new AssertionError("recovery must fail");
                    }

                    @Override
                    public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                        assertEquals(ExceptionsHelper.unwrap(e, IOException.class).getMessage(), "Expected failure");
                    }
                }),
                true,
                true,
                replicatePrimaryFunction
            )
        );
        closeShards(primarySource, primaryTarget);
    }

    // Todo: Remove this test when there is a better mechanism to test a functionality passing in different replication
    // strategy.
    public void testLockingBeforeAndAfterRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true, settings);
        final ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        CountDownLatch latch = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            latch.countDown();
            try {
                shard.relocated(routing.getTargetRelocatingShard().allocationId().getId(), primaryContext -> {}, () -> {});
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try (Releasable ignored = acquirePrimaryOperationPermitBlockingly(shard)) {
            // start finalization of recovery
            recoveryThread.start();
            latch.await();
            // recovery can only be finalized after we release the current primaryOperationLock
            assertFalse(shard.isRelocatedPrimary());
        }
        // recovery can be now finalized
        recoveryThread.join();
        assertTrue(shard.isRelocatedPrimary());
        final ExecutionException e = expectThrows(ExecutionException.class, () -> acquirePrimaryOperationPermitBlockingly(shard));
        assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
        assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));

        closeShards(shard);
    }

    // Todo: Remove this test when there is a better mechanism to test a functionality passing in different replication
    // strategy.
    public void testDelayedOperationsBeforeAndAfterRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true, settings);
        final ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        final CountDownLatch startRecovery = new CountDownLatch(1);
        final CountDownLatch relocationStarted = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            try {
                startRecovery.await();
                shard.relocated(
                    routing.getTargetRelocatingShard().allocationId().getId(),
                    primaryContext -> relocationStarted.countDown(),
                    () -> {}
                );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        recoveryThread.start();

        final int numberOfAcquisitions = randomIntBetween(1, 10);
        final List<Runnable> assertions = new ArrayList<>(numberOfAcquisitions);
        final int recoveryIndex = randomIntBetween(0, numberOfAcquisitions - 1);

        for (int i = 0; i < numberOfAcquisitions; i++) {
            final PlainActionFuture<Releasable> onLockAcquired;
            if (i < recoveryIndex) {
                final AtomicBoolean invoked = new AtomicBoolean();
                onLockAcquired = new PlainActionFuture<Releasable>() {

                    @Override
                    public void onResponse(Releasable releasable) {
                        invoked.set(true);
                        releasable.close();
                        super.onResponse(releasable);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError();
                    }

                };
                assertions.add(() -> assertTrue(invoked.get()));
            } else if (recoveryIndex == i) {
                startRecovery.countDown();
                relocationStarted.await();
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    final ExecutionException e = expectThrows(ExecutionException.class, () -> onLockAcquired.get(30, TimeUnit.SECONDS));
                    assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));
                });
            } else {
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    final ExecutionException e = expectThrows(ExecutionException.class, () -> onLockAcquired.get(30, TimeUnit.SECONDS));
                    assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));
                });
            }

            shard.acquirePrimaryOperationPermit(onLockAcquired, ThreadPool.Names.WRITE, "i_" + i);
        }

        for (final Runnable assertion : assertions) {
            assertion.run();
        }

        recoveryThread.join();

        closeShards(shard);
    }

    public void testReplicaReceivesLowerGeneration() throws Exception {
        // when a replica gets incoming segments that are lower than what it currently has on disk.

        // start 3 nodes Gens: P [2], R [2], R[2]
        // index some docs and flush twice, push to only 1 replica.
        // State Gens: P [4], R-1 [3], R-2 [2]
        // Promote R-2 as the new primary and demote the old primary.
        // State Gens: R[4], R-1 [3], P [4] - *commit on close of NRTEngine, xlog replayed and commit made.
        // index docs on new primary and flush
        // replicate to all.
        // Expected result: State Gens: P[4], R-1 [4], R-2 [4]
        try (ReplicationGroup shards = createGroup(2, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica_1 = shards.getReplicas().get(0);
            final IndexShard replica_2 = shards.getReplicas().get(1);
            int numDocs = randomIntBetween(10, 100);
            shards.indexDocs(numDocs);
            flushShard(primary, false);
            replicateSegments(primary, List.of(replica_1));
            numDocs = randomIntBetween(numDocs + 1, numDocs + 10);
            shards.indexDocs(numDocs);
            flushShard(primary, false);
            replicateSegments(primary, List.of(replica_1));

            assertEqualCommittedSegments(primary, replica_1);

            shards.promoteReplicaToPrimary(replica_2).get();
            primary.close("demoted", false, false);
            primary.store().close();
            IndexShard oldPrimary = shards.addReplicaWithExistingPath(primary.shardPath(), primary.routingEntry().currentNodeId());
            shards.recoverReplica(oldPrimary);

            numDocs = randomIntBetween(numDocs + 1, numDocs + 10);
            shards.indexDocs(numDocs);
            flushShard(replica_2, false);
            replicateSegments(replica_2, shards.getReplicas());
            assertEqualCommittedSegments(replica_2, oldPrimary, replica_1);
        }
    }

    public void testReplicaRestarts() throws Exception {
        try (ReplicationGroup shards = createGroup(3, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            // 1. Create ops that are in the index and xlog of both shards but not yet part of a commit point.
            final int numDocs = shards.indexDocs(randomInt(10));

            // refresh and copy the segments over.
            if (randomBoolean()) {
                flushShard(primary);
            }
            primary.refresh("Test");
            replicateSegments(primary, shards.getReplicas());

            // at this point both shards should have numDocs persisted and searchable.
            assertDocCounts(primary, numDocs, numDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, numDocs, numDocs);
            }

            final int i1 = randomInt(5);
            for (int i = 0; i < i1; i++) {
                shards.indexDocs(randomInt(10));

                // randomly resetart a replica
                final IndexShard replicaToRestart = getRandomReplica(shards);
                replicaToRestart.close("restart", false, false);
                replicaToRestart.store().close();
                shards.removeReplica(replicaToRestart);
                final IndexShard newReplica = shards.addReplicaWithExistingPath(
                    replicaToRestart.shardPath(),
                    replicaToRestart.routingEntry().currentNodeId()
                );
                shards.recoverReplica(newReplica);

                // refresh and push segments to our other replicas.
                if (randomBoolean()) {
                    failAndPromoteRandomReplica(shards);
                }
                flushShard(shards.getPrimary());
                replicateSegments(shards.getPrimary(), shards.getReplicas());
            }
            primary = shards.getPrimary();

            // refresh and push segments to our other replica.
            flushShard(primary);
            replicateSegments(primary, shards.getReplicas());

            for (IndexShard shard : shards) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final List<DocIdSeqNoAndSource> docsAfterReplication = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterReplication));
            }
        }
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryRefreshRefresh() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(false, false);
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryRefreshCommit() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(false, true);
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryCommitRefresh() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(true, false);
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryCommitCommit() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(true, true);
    }

    private void testNRTReplicaWithRemoteStorePromotedAsPrimary(boolean performFlushFirst, boolean performFlushSecond) throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "temp-fs")
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "temp-fs")
            .build();

        try (ReplicationGroup shards = createGroup(1, settings, indexMapping, new NRTReplicationEngineFactory(), createTempDir())) {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();
            final IndexShard nextPrimary = shards.getReplicas().get(0);

            // 1. Create ops that are in the index and xlog of both shards but not yet part of a commit point.
            final int numDocs = shards.indexDocs(randomInt(10));

            // refresh but do not copy the segments over.
            if (performFlushFirst) {
                flushShard(oldPrimary, true);
            } else {
                oldPrimary.refresh("Test");
            }
            // replicateSegments(primary, shards.getReplicas());

            // at this point both shards should have numDocs persisted and searchable.
            assertDocCounts(oldPrimary, numDocs, numDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, numDocs, 0);
            }

            // 2. Create ops that are in the replica's xlog, not in the index.
            // index some more into both but don't replicate. replica will have only numDocs searchable, but should have totalDocs
            // persisted.
            final int additonalDocs = shards.indexDocs(randomInt(10));
            final int totalDocs = numDocs + additonalDocs;

            if (performFlushSecond) {
                flushShard(oldPrimary, true);
            } else {
                oldPrimary.refresh("Test");
            }
            assertDocCounts(oldPrimary, totalDocs, totalDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, totalDocs, 0);
            }
            assertTrue(nextPrimary.translogStats().estimatedNumberOfOperations() >= additonalDocs);
            assertTrue(nextPrimary.translogStats().getUncommittedOperations() >= additonalDocs);

            int prevOperationCount = nextPrimary.translogStats().estimatedNumberOfOperations();

            // promote the replica
            shards.promoteReplicaToPrimary(nextPrimary).get();

            // close oldPrimary.
            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();

            assertEquals(InternalEngine.class, nextPrimary.getEngine().getClass());
            assertDocCounts(nextPrimary, totalDocs, totalDocs);

            // As we are downloading segments from remote segment store on failover, there should not be
            // any operations replayed from translog
            assertEquals(prevOperationCount, nextPrimary.translogStats().estimatedNumberOfOperations());

            // refresh and push segments to our other replica.
            nextPrimary.refresh("test");

            for (IndexShard shard : shards) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final List<DocIdSeqNoAndSource> docsAfterRecovery = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
            }
        }
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
            assertEqualTranslogOperations(shards, oldPrimary);

            // 2. Create ops that are in the replica's xlog, not in the index.
            // index some more into both but don't replicate. replica will have only numDocs searchable, but should have totalDocs
            // persisted.
            final int additonalDocs = shards.indexDocs(randomInt(10));
            final int totalDocs = numDocs + additonalDocs;

            assertDocCounts(oldPrimary, totalDocs, totalDocs);
            assertEqualTranslogOperations(shards, oldPrimary);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, totalDocs, numDocs);
            }
            assertEquals(totalDocs, oldPrimary.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, oldPrimary.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, nextPrimary.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, nextPrimary.translogStats().getUncommittedOperations());
            assertEquals(totalDocs, replica.translogStats().getUncommittedOperations());

            // promote the replica
            shards.syncGlobalCheckpoint();
            shards.promoteReplicaToPrimary(nextPrimary);

            // close and start the oldPrimary as a replica.
            oldPrimary.close("demoted", false, false);
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

    public void testReplicaPromotedWhileReplicating() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard nextPrimary = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            oldPrimary.refresh("Test");
            shards.syncGlobalCheckpoint();

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    resolveCheckpointInfoResponseListener(listener, oldPrimary);
                    ShardRouting oldRouting = nextPrimary.shardRouting;
                    try {
                        shards.promoteReplicaToPrimary(nextPrimary);
                    } catch (IOException e) {
                        Assert.fail("Promotion should not fail");
                    }
                    targetService.shardRoutingChanged(nextPrimary, oldRouting, nextPrimary.shardRouting);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(nextPrimary, targetService);
            // wait for replica to finish being promoted, and assert doc counts.
            final CountDownLatch latch = new CountDownLatch(1);
            nextPrimary.acquirePrimaryOperationPermit(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            }, ThreadPool.Names.GENERIC, "");
            latch.await();
            assertEquals(nextPrimary.getEngine().getClass(), InternalEngine.class);
            nextPrimary.refresh("test");

            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();
            IndexShard newReplica = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            assertDocCount(nextPrimary, numDocs);
            assertDocCount(newReplica, numDocs);

            nextPrimary.refresh("test");
            replicateSegments(nextPrimary, shards.getReplicas());
            final List<DocIdSeqNoAndSource> docsAfterRecovery = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
            }
        }
    }

    public void testReplicaClosesWhileReplicating_AfterGetCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    // trigger a cancellation by closing the replica.
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                    resolveCheckpointInfoResponseListener(listener, primary);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    Assert.fail("Should not be reached");
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testReplicaClosesWhileReplicating_AfterGetSegmentFiles() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    resolveCheckpointInfoResponseListener(listener, primary);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    // randomly resolve the listener, indicating the source has resolved.
                    listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testCloseShardDuringFinalize() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final IndexShard replicaSpy = spy(replica);

            primary.refresh("Test");

            doThrow(AlreadyClosedException.class).when(replicaSpy).finalizeReplication(any());

            replicateSegments(primary, List.of(replicaSpy));
        }
    }

    public void testCloseShardWhileGettingCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {

                ActionListener<CheckpointInfoResponse> listener;

                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    // set the listener, we will only fail it once we cancel the source.
                    this.listener = listener;
                    // shard is closing while we are copying files.
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    Assert.fail("Unreachable");
                }

                @Override
                public void cancel() {
                    // simulate listener resolving, but only after we have issued a cancel from beforeIndexShardClosed .
                    final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("retryable action was cancelled");
                    listener.onFailure(exception);
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testBeforeIndexShardClosedWhileCopyingFiles() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {

                ActionListener<GetSegmentFilesResponse> listener;

                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    resolveCheckpointInfoResponseListener(listener, primary);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    // set the listener, we will only fail it once we cancel the source.
                    this.listener = listener;
                    // shard is closing while we are copying files.
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                }

                @Override
                public void cancel() {
                    // simulate listener resolving, but only after we have issued a cancel from beforeIndexShardClosed .
                    final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("retryable action was cancelled");
                    listener.onFailure(exception);
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testPrimaryCancelsExecution() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    listener.onFailure(new CancellableThreads.ExecutionCancelledException("Cancelled"));
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {}
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    private SegmentReplicationTargetService newTargetService(SegmentReplicationSourceFactory sourceFactory) {
        return new SegmentReplicationTargetService(
            threadPool,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(TransportService.class),
            sourceFactory,
            null,
            null
        );
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
    }

    private void resolveCheckpointInfoResponseListener(ActionListener<CheckpointInfoResponse> listener, IndexShard primary) {
        try {
            final CopyState copyState = new CopyState(
                ReplicationCheckpoint.empty(primary.shardId, primary.getLatestReplicationCheckpoint().getCodec()),
                primary
            );
            listener.onResponse(
                new CheckpointInfoResponse(copyState.getCheckpoint(), copyState.getMetadataMap(), copyState.getInfosBytes())
            );
        } catch (IOException e) {
            logger.error("Unexpected error computing CopyState", e);
            Assert.fail("Failed to compute copyState");
        }
    }

    private void startReplicationAndAssertCancellation(IndexShard replica, SegmentReplicationTargetService targetService)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final SegmentReplicationTarget target = targetService.startReplication(
            replica,
            new SegmentReplicationTargetService.SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {
                    Assert.fail("Replication should not complete");
                }

                @Override
                public void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                    assertFalse(sendShardFailure);
                    latch.countDown();
                }
            }
        );

        latch.await(2, TimeUnit.SECONDS);
        assertEquals("Should have resolved listener with failure", 0, latch.getCount());
        assertNull(targetService.get(target.getId()));
    }

    private IndexShard getRandomReplica(ReplicationGroup shards) {
        return shards.getReplicas().get(randomInt(shards.getReplicas().size() - 1));
    }

    private IndexShard failAndPromoteRandomReplica(ReplicationGroup shards) throws IOException {
        IndexShard primary = shards.getPrimary();
        final IndexShard newPrimary = getRandomReplica(shards);
        shards.promoteReplicaToPrimary(newPrimary);
        primary.close("demoted", true, false);
        primary.store().close();
        primary = shards.addReplicaWithExistingPath(primary.shardPath(), primary.routingEntry().currentNodeId());
        shards.recoverReplica(primary);
        return newPrimary;
    }

    private void assertEqualCommittedSegments(IndexShard primary, IndexShard... replicas) throws IOException {
        for (IndexShard replica : replicas) {
            final SegmentInfos replicaInfos = replica.store().readLastCommittedSegmentsInfo();
            final SegmentInfos primaryInfos = primary.store().readLastCommittedSegmentsInfo();
            final Map<String, StoreFileMetadata> latestReplicaMetadata = replica.store().getSegmentMetadataMap(replicaInfos);
            final Map<String, StoreFileMetadata> latestPrimaryMetadata = primary.store().getSegmentMetadataMap(primaryInfos);
            final Store.RecoveryDiff diff = Store.segmentReplicationDiff(latestPrimaryMetadata, latestReplicaMetadata);
            assertTrue(diff.different.isEmpty());
            assertTrue(diff.missing.isEmpty());
        }
    }

    private void assertEqualTranslogOperations(ReplicationGroup shards, IndexShard primaryShard) throws IOException {
        try (final Translog.Snapshot snapshot = getTranslog(primaryShard).newSnapshot()) {
            List<Translog.Operation> operations = new ArrayList<>();
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                final Translog.Operation newOp = op;
                operations.add(newOp);
            }
            for (IndexShard replica : shards.getReplicas()) {
                try (final Translog.Snapshot replicaSnapshot = getTranslog(replica).newSnapshot()) {
                    assertThat(replicaSnapshot, SnapshotMatchers.containsOperationsInAnyOrder(operations));
                }
            }
        }
    }
}
