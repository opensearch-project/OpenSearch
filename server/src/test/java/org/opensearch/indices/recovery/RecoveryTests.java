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

package org.opensearch.indices.recovery;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.InternalEngineTests;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.replication.RecoveryDuringReplicationTests;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.SnapshotMatchers;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RecoveryTests extends OpenSearchIndexLevelReplicationTestCase {

    public void testTranslogHistoryTransferred() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            int docs = shards.indexDocs(10);
            getTranslog(shards.getPrimary()).rollGeneration();
            shards.flush();
            int moreDocs = shards.indexDocs(randomInt(10));
            shards.addReplica();
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            boolean softDeletesEnabled = replica.indexSettings().isSoftDeleteEnabled();
            assertThat(getTranslog(replica).totalOperations(), equalTo(softDeletesEnabled ? 0 : docs + moreDocs));
            shards.assertAllEqual(docs + moreDocs);
        }
    }

    public void testWithSegmentReplication_ReplicaUsesPrimaryTranslogUUID() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
        try (ReplicationGroup shards = createGroup(2, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final String expectedUUID = getTranslog(shards.getPrimary()).getTranslogUUID();
            assertTrue(
                shards.getReplicas().stream().allMatch(indexShard -> getTranslog(indexShard).getTranslogUUID().equals(expectedUUID))
            );
        }
    }

    public void testRetentionPolicyChangeDuringRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            shards.indexDocs(10);
            getTranslog(shards.getPrimary()).rollGeneration();
            shards.flush();
            shards.indexDocs(10);
            final IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);
            Future<Void> future = shards.asyncRecoverReplica(
                replica,
                (indexShard, node) -> new RecoveryDuringReplicationTests.BlockingTarget(
                    RecoveryState.Stage.TRANSLOG,
                    recoveryBlocked,
                    releaseRecovery,
                    indexShard,
                    node,
                    recoveryListener,
                    logger,
                    threadPool
                )
            );
            recoveryBlocked.await();
            IndexMetadata.Builder builder = IndexMetadata.builder(replica.indexSettings().getIndexMetadata());
            builder.settings(
                Settings.builder()
                    .put(replica.indexSettings().getSettings())
                    .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1")
                    .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1")
                    // force a roll and flush
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "100b")
            );
            replica.indexSettings().updateIndexMetadata(builder.build());
            replica.onSettingsChanged();
            releaseRecovery.countDown();
            future.get();
            // rolling/flushing is async
            assertBusy(() -> {
                assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo(19L));
                assertThat(getTranslog(replica).totalOperations(), equalTo(0));
            });
        }
    }

    public void testRecoveryWithOutOfOrderDeleteWithSoftDeletes() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 10)
            // If soft-deletes is enabled, delete#1 will be reclaimed because its segment (segment_1) is fully deleted
            // index#0 will be retained if merge is disabled; otherwise it will be reclaimed because gcp=3 and retained_ops=0
            .put(MergePolicyProvider.INDEX_MERGE_ENABLED, false)
            .build();
        try (ReplicationGroup shards = createGroup(1, settings)) {
            shards.startAll();
            // create out of order delete and index op on replica
            final IndexShard orgReplica = shards.getReplicas().get(0);
            final String indexName = orgReplica.shardId().getIndexName();
            final long primaryTerm = orgReplica.getOperationPrimaryTerm();

            // delete #1
            orgReplica.advanceMaxSeqNoOfUpdatesOrDeletes(1); // manually advance msu for this delete
            orgReplica.applyDeleteOperationOnReplica(1, primaryTerm, 2, "id");
            orgReplica.flush(new FlushRequest().force(true)); // isolate delete#1 in its own translog generation and lucene segment
            // index #0
            orgReplica.applyIndexOperationOnReplica(
                UUID.randomUUID().toString(),
                0,
                primaryTerm,
                1,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                new SourceToParse(indexName, "id", new BytesArray("{}"), MediaTypeRegistry.JSON),
                null
            );
            // index #3
            orgReplica.applyIndexOperationOnReplica(
                UUID.randomUUID().toString(),
                3,
                primaryTerm,
                1,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                new SourceToParse(indexName, "id-3", new BytesArray("{}"), MediaTypeRegistry.JSON),
                null
            );
            // Flushing a new commit with local checkpoint=1 allows to delete the translog gen #1.
            orgReplica.flush(new FlushRequest().force(true).waitIfOngoing(true));
            // index #2
            orgReplica.applyIndexOperationOnReplica(
                UUID.randomUUID().toString(),
                2,
                primaryTerm,
                1,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                new SourceToParse(indexName, "id-2", new BytesArray("{}"), MediaTypeRegistry.JSON),
                null
            );
            orgReplica.sync(); // advance local checkpoint
            orgReplica.updateGlobalCheckpointOnReplica(3L, "test");
            // index #5 -> force NoOp #4.
            orgReplica.applyIndexOperationOnReplica(
                UUID.randomUUID().toString(),
                5,
                primaryTerm,
                1,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                new SourceToParse(indexName, "id-5", new BytesArray("{}"), MediaTypeRegistry.JSON),
                null
            );

            if (randomBoolean()) {
                if (randomBoolean()) {
                    logger.info("--> flushing shard (translog/soft-deletes will be trimmed)");
                    IndexMetadata.Builder builder = IndexMetadata.builder(orgReplica.indexSettings().getIndexMetadata());
                    builder.settings(
                        Settings.builder()
                            .put(orgReplica.indexSettings().getSettings())
                            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)
                    );
                    orgReplica.indexSettings().updateIndexMetadata(builder.build());
                    orgReplica.onSettingsChanged();
                }
                flushShard(orgReplica);
            }

            final IndexShard orgPrimary = shards.getPrimary();
            shards.promoteReplicaToPrimary(orgReplica).get(); // wait for primary/replica sync to make sure seq# gap is closed.

            IndexShard newReplica = shards.addReplicaWithExistingPath(orgPrimary.shardPath(), orgPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            shards.assertAllEqual(3);
            try (Translog.Snapshot snapshot = newReplica.newChangesSnapshot("test", 0, Long.MAX_VALUE, false, randomBoolean())) {
                assertThat(snapshot, SnapshotMatchers.size(6));
            }
        }
    }

    public void testDifferentHistoryUUIDDisablesOPsRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            // index some shared docs
            final int flushedDocs = 10;
            final int nonFlushedDocs = randomIntBetween(0, 10);
            final int numDocs = flushedDocs + nonFlushedDocs;
            shards.indexDocs(flushedDocs);
            shards.flush();
            shards.indexDocs(nonFlushedDocs);

            IndexShard replica = shards.getReplicas().get(0);
            final String historyUUID = replica.getHistoryUUID();
            Translog.TranslogGeneration translogGeneration = getTranslog(replica).getGeneration();
            shards.removeReplica(replica);
            replica.close("test", false, false);
            IndexWriterConfig iwc = new IndexWriterConfig(null).setCommitOnClose(false)
                // we don't want merges to happen here - we call maybe merge on the engine
                // later once we stared it up otherwise we would need to wait for it here
                // we also don't specify a codec here and merges should use the engines for this index
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            Map<String, String> userData = new HashMap<>(replica.store().readLastCommittedSegmentsInfo().getUserData());
            final String translogUUIDtoUse;
            final String historyUUIDtoUse = UUIDs.randomBase64UUID(random());
            if (randomBoolean()) {
                // create a new translog
                translogUUIDtoUse = Translog.createEmptyTranslog(
                    replica.shardPath().resolveTranslog(),
                    flushedDocs,
                    replica.shardId(),
                    replica.getPendingPrimaryTerm()
                );
            } else {
                translogUUIDtoUse = translogGeneration.translogUUID;
            }
            try (IndexWriter writer = new IndexWriter(replica.store().directory(), iwc)) {
                userData.put(Engine.HISTORY_UUID_KEY, historyUUIDtoUse);
                userData.put(Translog.TRANSLOG_UUID_KEY, translogUUIDtoUse);
                writer.setLiveCommitData(userData.entrySet());
                writer.commit();
            }
            replica.store().close();
            IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            // file based recovery should be made
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), not(empty()));
            boolean softDeletesEnabled = replica.indexSettings().isSoftDeleteEnabled();
            assertThat(getTranslog(newReplica).totalOperations(), equalTo(softDeletesEnabled ? 0 : numDocs));

            // history uuid was restored
            assertThat(newReplica.getHistoryUUID(), equalTo(historyUUID));
            assertThat(newReplica.commitStats().getUserData().get(Engine.HISTORY_UUID_KEY), equalTo(historyUUID));

            shards.assertAllEqual(numDocs);
        }
    }

    public void testPeerRecoveryPersistGlobalCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            final long numDocs = shards.indexDocs(between(1, 100));
            if (randomBoolean()) {
                shards.flush();
            }
            final IndexShard replica = shards.addReplica();
            shards.recoverReplica(replica);
            assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo(numDocs - 1));
        }
    }

    public void testPeerRecoverySendSafeCommitInFileBased() throws Exception {
        IndexShard primaryShard = newStartedShard(true);
        int numDocs = between(1, 100);
        long globalCheckpoint = 0;
        for (int i = 0; i < numDocs; i++) {
            Engine.IndexResult result = primaryShard.applyIndexOperationOnPrimary(
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                new SourceToParse(primaryShard.shardId().getIndexName(), Integer.toString(i), new BytesArray("{}"), MediaTypeRegistry.JSON),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false
            );
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            if (randomBoolean()) {
                globalCheckpoint = randomLongBetween(globalCheckpoint, i);
                primaryShard.updateLocalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.updateGlobalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.flush(new FlushRequest());
            }
        }
        IndexShard replicaShard = newShard(primaryShard.shardId(), false);
        updateMappings(replicaShard, primaryShard.indexSettings().getIndexMetadata());
        recoverReplica(replicaShard, primaryShard, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, threadPool) {
            @Override
            public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
                super.prepareForTranslogOperations(totalTranslogOps, listener);
                assertThat(replicaShard.getLastKnownGlobalCheckpoint(), equalTo(primaryShard.getLastKnownGlobalCheckpoint()));
            }

            @Override
            public void cleanFiles(
                int totalTranslogOps,
                long globalCheckpoint,
                Store.MetadataSnapshot sourceMetadata,
                ActionListener<Void> listener
            ) {
                assertThat(globalCheckpoint, equalTo(primaryShard.getLastKnownGlobalCheckpoint()));
                super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, listener);
            }
        }, true, true);
        List<IndexCommit> commits = DirectoryReader.listCommits(replicaShard.store().directory());
        long maxSeqNo = Long.parseLong(commits.get(0).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
        assertThat(maxSeqNo, lessThanOrEqualTo(globalCheckpoint));
        closeShards(primaryShard, replicaShard);
    }

    public void testSequenceBasedRecoveryKeepsTranslog() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            final int initDocs = scaledRandomIntBetween(0, 20);
            int uncommittedDocs = 0;
            for (int i = 0; i < initDocs; i++) {
                shards.indexDocs(1);
                uncommittedDocs++;
                if (randomBoolean()) {
                    shards.syncGlobalCheckpoint();
                    shards.flush();
                    uncommittedDocs = 0;
                }
            }
            shards.removeReplica(replica);
            final int moreDocs = shards.indexDocs(scaledRandomIntBetween(0, 20));
            if (randomBoolean()) {
                shards.flush();
            }
            replica.close("test", randomBoolean(), false);
            replica.store().close();
            final IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            try (Translog.Snapshot snapshot = getTranslog(newReplica).newSnapshot()) {
                if (newReplica.indexSettings().isSoftDeleteEnabled()) {
                    assertThat(snapshot.totalOperations(), equalTo(0));
                } else {
                    assertThat(
                        "Sequence based recovery should keep existing translog",
                        snapshot,
                        SnapshotMatchers.size(initDocs + moreDocs)
                    );
                }
            }
            assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(uncommittedDocs + moreDocs));
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
        }
    }

    /**
     * This test makes sure that there is no infinite loop of flushing (the condition `shouldPeriodicallyFlush` eventually is false)
     * in peer-recovery if a primary sends a fully-baked index commit.
     */
    public void testShouldFlushAfterPeerRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            int numDocs = shards.indexDocs(between(10, 100));
            final long translogSizeOnPrimary = shards.getPrimary().translogStats().getUncommittedSizeInBytes();
            shards.flush();

            final IndexShard replica = shards.addReplica();
            IndexMetadata.Builder builder = IndexMetadata.builder(replica.indexSettings().getIndexMetadata());
            long flushThreshold = RandomNumbers.randomLongBetween(random(), 100, translogSizeOnPrimary);
            builder.settings(
                Settings.builder()
                    .put(replica.indexSettings().getSettings())
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), flushThreshold + "b")
            );
            replica.indexSettings().updateIndexMetadata(builder.build());
            replica.onSettingsChanged();
            shards.recoverReplica(replica);
            // Make sure the flushing will eventually be completed (eg. `shouldPeriodicallyFlush` is false)
            assertBusy(() -> assertThat(getEngine(replica).shouldPeriodicallyFlush(), equalTo(false)));
            boolean softDeletesEnabled = replica.indexSettings().isSoftDeleteEnabled();
            assertThat(getTranslog(replica).totalOperations(), equalTo(softDeletesEnabled ? 0 : numDocs));
            shards.assertAllEqual(numDocs);
        }
    }

    public void testFailsToIndexDuringPeerRecovery() throws Exception {
        AtomicReference<IOException> throwExceptionDuringIndexing = new AtomicReference<>(new IOException("simulated"));
        try (ReplicationGroup group = new ReplicationGroup(buildIndexMetadata(0)) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                if (routing.primary()) {
                    return new InternalEngineFactory();
                } else {
                    return config -> InternalEngineTests.createInternalEngine((dir, iwc) -> new IndexWriter(dir, iwc) {
                        @Override
                        public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
                            final IOException error = throwExceptionDuringIndexing.getAndSet(null);
                            if (error != null) {
                                throw error;
                            }
                            return super.addDocument(doc);
                        }
                    }, null, null, config);
                }
            }
        }) {
            group.startAll();
            group.indexDocs(randomIntBetween(1, 10));
            allowShardFailures();
            IndexShard replica = group.addReplica();
            expectThrows(
                Exception.class,
                () -> group.recoverReplica(replica, (shard, sourceNode) -> new RecoveryTarget(shard, sourceNode, new ReplicationListener() {
                    @Override
                    public void onDone(ReplicationState state) {
                        throw new AssertionError("recovery must fail");
                    }

                    @Override
                    public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                        assertThat(ExceptionsHelper.unwrap(e, IOException.class).getMessage(), equalTo("simulated"));
                    }
                }, threadPool))
            );
            expectThrows(AlreadyClosedException.class, () -> replica.refresh("test"));
            group.removeReplica(replica);
            replica.store().close();
            closeShards(replica);
        }
    }

    public void testRecoveryTrimsLocalTranslog() throws Exception {
        try (ReplicationGroup shards = createGroup(between(1, 2))) {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();
            shards.indexDocs(scaledRandomIntBetween(1, 100));
            if (randomBoolean()) {
                shards.flush();
            }
            int inflightDocs = scaledRandomIntBetween(1, 100);
            for (int i = 0; i < inflightDocs; i++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName()).id("extra_" + i).source("{}", MediaTypeRegistry.JSON);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                for (IndexShard replica : randomSubsetOf(shards.getReplicas())) {
                    indexOnReplica(bulkShardRequest, shards, replica);
                }
                if (rarely()) {
                    shards.flush();
                }
            }
            shards.syncGlobalCheckpoint();
            shards.promoteReplicaToPrimary(randomFrom(shards.getReplicas())).get();
            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();
            oldPrimary = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(oldPrimary);
            for (IndexShard shard : shards) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final List<DocIdSeqNoAndSource> docsAfterRecovery = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
            }
            shards.promoteReplicaToPrimary(oldPrimary).get();
            for (IndexShard shard : shards) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
        }
    }
}
