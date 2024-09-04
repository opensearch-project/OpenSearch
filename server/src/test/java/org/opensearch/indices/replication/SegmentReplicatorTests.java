/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentReplicatorTests extends IndexShardTestCase {

    public void testStartReplicationWithoutSourceFactory() {
        ThreadPool threadpool = mock(ThreadPool.class);
        ExecutorService mock = mock(ExecutorService.class);
        when(threadpool.generic()).thenReturn(mock);
        SegmentReplicator segmentReplicator = new SegmentReplicator(threadpool);

        IndexShard shard = mock(IndexShard.class);
        segmentReplicator.startReplication(shard);
        Mockito.verifyNoInteractions(mock);
    }

    public void testStartReplicationRuns() throws IOException {
        ThreadPool threadpool = mock(ThreadPool.class);
        ExecutorService mock = mock(ExecutorService.class);
        when(threadpool.generic()).thenReturn(mock);
        final IndexShard indexShard = newStartedShard(randomBoolean(), Settings.EMPTY, new NRTReplicationEngineFactory());
        SegmentReplicator segmentReplicator = spy(new SegmentReplicator(threadpool));
        SegmentReplicationSourceFactory factory = mock(SegmentReplicationSourceFactory.class);
        when(factory.get(indexShard)).thenReturn(new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {}

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {}
        });
        segmentReplicator.setSourceFactory(factory);
        segmentReplicator.startReplication(indexShard);
        verify(mock, times(1)).execute(any());

        // this startReplication entrypoint creates a SegmentReplicationTarget that incref's the shard's store
        // but it is actually run from this test bc we mock the executor. We just want to test that it is
        // created and passed to the threadpool for execution, so close the store here.
        indexShard.store().decRef();
        closeShards(indexShard);
    }
}
