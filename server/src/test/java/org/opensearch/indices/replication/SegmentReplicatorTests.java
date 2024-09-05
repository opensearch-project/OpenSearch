/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.IOContext;
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SegmentReplicatorTests extends IndexShardTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    public void testStartReplicationWithoutSourceFactory() {
        ThreadPool threadpool = mock(ThreadPool.class);
        ExecutorService mock = mock(ExecutorService.class);
        when(threadpool.generic()).thenReturn(mock);
        SegmentReplicator segmentReplicator = new SegmentReplicator(threadpool);

        IndexShard shard = mock(IndexShard.class);
        segmentReplicator.startReplication(shard);
        Mockito.verifyNoInteractions(mock);
    }

    public void testStartReplicationRunsSuccessfully() throws Exception {
        final IndexShard replica = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        final IndexShard primary = newStartedShard(true, settings, new NRTReplicationEngineFactory());

        // index and copy segments to replica.
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(primary, "_doc", Integer.toString(i));
        }
        primary.refresh("test");

        SegmentReplicator segmentReplicator = spy(new SegmentReplicator(threadPool));
        SegmentReplicationSourceFactory factory = mock(SegmentReplicationSourceFactory.class);
        when(factory.get(replica)).thenReturn(new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                resolveCheckpointListener(listener, primary);
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                try {
                    Lucene.cleanLuceneIndex(indexShard.store().directory());
                    Map<String, StoreFileMetadata> segmentMetadataMap = primary.getSegmentMetadataMap();
                    for (String file : segmentMetadataMap.keySet()) {
                        indexShard.store().directory().copyFrom(primary.store().directory(), file, file, IOContext.DEFAULT);
                    }
                    listener.onResponse(new GetSegmentFilesResponse(new ArrayList<>(segmentMetadataMap.values())));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        segmentReplicator.setSourceFactory(factory);
        segmentReplicator.startReplication(replica);
        assertBusy(() -> assertDocCount(replica, numDocs));
        closeShards(primary, replica);
    }

    public void testReplicationFails() throws Exception {
        allowShardFailures();
        final IndexShard replica = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        final IndexShard primary = newStartedShard(true, settings, new NRTReplicationEngineFactory());

        SegmentReplicator segmentReplicator = spy(new SegmentReplicator(threadPool));
        SegmentReplicationSourceFactory factory = mock(SegmentReplicationSourceFactory.class);
        when(factory.get(replica)).thenReturn(new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                resolveCheckpointListener(listener, primary);
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                OpenSearchCorruptionException corruptIndexException = new OpenSearchCorruptionException("test");
                try {
                    indexShard.store().markStoreCorrupted(corruptIndexException);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                listener.onFailure(corruptIndexException);
            }
        });
        // assert shard failure on corruption
        AtomicBoolean failureCallbackTriggered = new AtomicBoolean(false);
        replica.addShardFailureCallback((ig) -> failureCallbackTriggered.set(true));
        segmentReplicator.setSourceFactory(factory);
        segmentReplicator.startReplication(replica);
        assertBusy(() -> assertTrue(failureCallbackTriggered.get()));
        closeShards(primary, replica);
    }

    protected void resolveCheckpointListener(ActionListener<CheckpointInfoResponse> listener, IndexShard primary) {
        try (final CopyState copyState = new CopyState(primary)) {
            listener.onResponse(
                new CheckpointInfoResponse(copyState.getCheckpoint(), copyState.getMetadataMap(), copyState.getInfosBytes())
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
