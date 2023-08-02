/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.junit.Assert;
import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.routing.TestShardRouting.newShardRouting;

public class ReplicaRecoveryWithRemoteTranslogOnPrimaryTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, "true")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "translog-repo")
        .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
        .build();

    @Before
    public void setup() {
        // Todo: Remove feature flag once remote store integration with segrep goes GA
        FeatureFlags.initializeFeatureFlags(
            Settings.builder().put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL_SETTING.getKey(), "true").build()
        );
    }

    public void testStartSequenceForReplicaRecovery() throws Exception {
        final Path remoteDir = createTempDir();
        final String indexMapping = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": {} }";
        try (ReplicationGroup shards = createGroup(0, settings, indexMapping, new NRTReplicationEngineFactory(), remoteDir)) {
            shards.startPrimary();
            final IndexShard primary = shards.getPrimary();
            int numDocs = shards.indexDocs(randomIntBetween(10, 100));
            shards.flush();

            final IndexShard replica = shards.addReplica(remoteDir);
            shards.startAll();

            allowShardFailures();
            replica.failShard("test", null);

            final ShardRouting replicaRouting = replica.routingEntry();
            final IndexMetadata newIndexMetadata = IndexMetadata.builder(replica.indexSettings().getIndexMetadata())
                .primaryTerm(replicaRouting.shardId().id(), replica.getOperationPrimaryTerm() + 1)
                .build();
            closeShards(replica);
            shards.removeReplica(replica);

            int moreDocs = shards.indexDocs(randomIntBetween(20, 100));
            shards.flush();

            final ShardRouting replicaRouting2 = newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                false,
                ShardRoutingState.INITIALIZING,
                RecoverySource.PeerRecoverySource.INSTANCE
            );
            Store remoteStore = createRemoteStore(remoteDir, replicaRouting2, newIndexMetadata);
            IndexShard newReplicaShard = newShard(
                newShardRouting(
                    replicaRouting.shardId(),
                    replicaRouting.currentNodeId(),
                    false,
                    ShardRoutingState.INITIALIZING,
                    RecoverySource.PeerRecoverySource.INSTANCE
                ),
                replica.shardPath(),
                newIndexMetadata,
                null,
                null,
                replica.getEngineFactory(),
                replica.getEngineConfigFactory(),
                replica.getGlobalCheckpointSyncer(),
                replica.getRetentionLeaseSyncer(),
                EMPTY_EVENT_LISTENER,
                remoteStore
            );
            shards.addReplica(newReplicaShard);
            AtomicBoolean assertDone = new AtomicBoolean(false);
            shards.recoverReplica(newReplicaShard, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener) {
                @Override
                public IndexShard indexShard() {
                    IndexShard idxShard = super.indexShard();
                    if (assertDone.compareAndSet(false, true)) {
                        // verify the starting sequence number while recovering a failed shard which has a valid last commit
                        long startingSeqNo = -1;
                        try {
                            startingSeqNo = Long.parseLong(
                                idxShard.store().readLastCommittedSegmentsInfo().getUserData().get(SequenceNumbers.MAX_SEQ_NO)
                            );
                        } catch (IOException e) {
                            Assert.fail();
                        }
                        assertEquals(numDocs - 1, startingSeqNo);
                    }
                    return idxShard;
                }
            });
            shards.flush();
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs + moreDocs);
        }
    }

    public void testNoTranslogHistoryTransferred() throws Exception {
        final Path remoteDir = createTempDir();
        final String indexMapping = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": {} }";
        try (ReplicationGroup shards = createGroup(0, settings, indexMapping, new NRTReplicationEngineFactory(), remoteDir)) {

            // Step1 - Start primary, index docs, flush, index more docs, check translog in primary as expected
            shards.startPrimary();
            final IndexShard primary = shards.getPrimary();
            int numDocs = shards.indexDocs(randomIntBetween(10, 100));
            shards.flush();
            List<DocIdSeqNoAndSource> docIdAndSeqNosAfterFlush = getDocIdAndSeqNos(primary);
            int moreDocs = shards.indexDocs(randomIntBetween(20, 100));
            assertEquals(numDocs + moreDocs, getTranslog(primary).totalOperations());

            // Step 2 - Start replica, recovery happens, check docs recovered till last flush
            final IndexShard replica = shards.addReplica(remoteDir);
            shards.startAll();
            assertEquals(docIdAndSeqNosAfterFlush, getDocIdAndSeqNos(replica));
            assertDocCount(replica, numDocs);
            assertEquals(NRTReplicationEngine.class, replica.getEngine().getClass());

            // Step 3 - Check replica's translog has no operations
            assertEquals(WriteOnlyTranslogManager.class, replica.getEngine().translogManager().getClass());
            WriteOnlyTranslogManager replicaTranslogManager = (WriteOnlyTranslogManager) replica.getEngine().translogManager();
            assertEquals(0, replicaTranslogManager.getTranslog().totalOperations());

            // Adding this for close to succeed
            shards.flush();
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs + moreDocs);
        }
    }
}
