/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteStorePeerRecoverySourceHandlerTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
    );
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);

    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, "true")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "translog-repo")
        .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
        .build();

    public void testReplicaShardRecoveryUptoLastFlushedCommit() throws Exception {
        final Path remoteDir = createTempDir();
        final String indexMapping = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": {} }";
        try (ReplicationGroup shards = createGroup(0, settings, indexMapping, new NRTReplicationEngineFactory(), remoteDir)) {

            // Step1 - Start primary, index docs and flush
            shards.startPrimary();
            final IndexShard primary = shards.getPrimary();
            int numDocs = shards.indexDocs(randomIntBetween(10, 20));
            logger.info("--> Index numDocs {} and flush", numDocs);
            shards.flush();

            // Step 2 - Start replica for recovery to happen, check both has same number of docs
            final IndexShard replica1 = shards.addReplica(remoteDir);
            logger.info("--> Added and started replica {}", replica1.routingEntry());
            shards.startAll();
            assertEquals(getDocIdAndSeqNos(primary), getDocIdAndSeqNos(replica1));

            // Step 3 - Index more docs, run segment replication, check both have same number of docs
            int moreDocs = shards.indexDocs(randomIntBetween(10, 20));
            primary.refresh("test");
            logger.info("--> Index more docs {} and replicate segments", moreDocs);
            replicateSegments(primary, shards.getReplicas());
            assertEquals(getDocIdAndSeqNos(primary), getDocIdAndSeqNos(replica1));

            // Step 4 - Check both shard has expected number of doc count
            assertDocCount(primary, numDocs + moreDocs);
            assertDocCount(replica1, numDocs + moreDocs);

            // Step 5 - Check retention lease does not exist for the replica shard
            assertEquals(1, primary.getRetentionLeases().leases().size());
            assertFalse(primary.getRetentionLeases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replica1.routingEntry())));

            // Step 6 - Start new replica, recovery happens, and check that new replica has all docs
            final IndexShard replica2 = shards.addReplica(remoteDir);
            logger.info("--> Added and started replica {}", replica2.routingEntry());
            shards.startAll();
            shards.assertAllEqual(numDocs + moreDocs);

            // Step 7 - Check retention lease does not exist for the replica shard
            assertEquals(1, primary.getRetentionLeases().leases().size());
            assertFalse(primary.getRetentionLeases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replica2.routingEntry())));
        }
    }

    public StartRecoveryRequest getStartRecoveryRequest() throws IOException {
        Store.MetadataSnapshot metadataSnapshot = randomBoolean()
            ? Store.MetadataSnapshot.EMPTY
            : new Store.MetadataSnapshot(
            Collections.emptyMap(),
            Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()),
            randomIntBetween(0, 100)
        );
        return new StartRecoveryRequest(
            shardId,
            null,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong()
        );
    }


    public void testThrowExceptionOnNoTargetInRouting() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean(), anyBoolean())).thenReturn(mock(SegmentsStats.class));
        when(shard.isRelocatedPrimary()).thenReturn(false);
        final org.opensearch.index.shard.ReplicationGroup replicationGroup = mock(org.opensearch.index.shard.ReplicationGroup.class);
        final IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.getByAllocationId(anyString())).thenReturn(null);
        when(shard.getReplicationGroup()).thenReturn(replicationGroup);
        when(replicationGroup.getRoutingTable()).thenReturn(routingTable);
        when(shard.acquireSafeIndexCommit()).thenReturn(mock(GatedCloseable.class));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>) invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), any());

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 5))
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            );
        if (randomBoolean()) {
            indexMetadata.state(IndexMetadata.State.CLOSE);
        }
        when(shard.indexSettings()).thenReturn(new IndexSettings(indexMetadata.build(), Settings.EMPTY));

        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new RemoteStorePeerRecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 8),
            between(1, 8)
        ) {

            @Override
            void phase1(
                IndexCommit snapshot,
                long startingSeqNo,
                IntSupplier translogOps,
                ActionListener<SendFileResult> listener,
                boolean skipCreateRetentionLeaseStep
            ) {
                phase1Called.set(true);
                super.phase1(snapshot, startingSeqNo, translogOps, listener, skipCreateRetentionLeaseStep);
            }

            @Override
            void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
                prepareTargetForTranslogCalled.set(true);
                super.prepareTargetForTranslog(totalTranslogOps, listener);
            }

            @Override
            void phase2(
                long startingSeqNo,
                long endingSeqNo,
                Translog.Snapshot snapshot,
                long maxSeenAutoIdTimestamp,
                long maxSeqNoOfUpdatesOrDeletes,
                RetentionLeases retentionLeases,
                long mappingVersion,
                ActionListener<SendSnapshotResult> listener
            ) throws IOException {
                phase2Called.set(true);
                super.phase2(
                    startingSeqNo,
                    endingSeqNo,
                    snapshot,
                    maxSeenAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersion,
                    listener
                );
            }

        };
        PlainActionFuture<RecoveryResponse> future = new PlainActionFuture<>();
        expectThrows(DelayRecoveryException.class, () -> {
            handler.recoverToTarget(future);
            future.actionGet();
        });
        verify(routingTable, times(3)).getByAllocationId(null);
        assertFalse(phase1Called.get());
        assertFalse(prepareTargetForTranslogCalled.get());
        assertFalse(phase2Called.get());
    }
}
