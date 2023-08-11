/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.FilterDirectory;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.RemoteStoreRefreshListenerTests;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteStoreReplicationSourceTests extends OpenSearchIndexLevelReplicationTestCase {
    private static final long REPLICATION_ID = 123L;
    private RemoteStoreReplicationSource replicationSource;
    private IndexShard primaryShard;

    private IndexShard replicaShard;
    private final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
        .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-repo")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-repo")
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryShard = newStartedShard(true, settings, new InternalEngineFactory());
        indexDoc(primaryShard, "_doc", "1");
        indexDoc(primaryShard, "_doc", "2");
        primaryShard.refresh("test");
        replicaShard = newStartedShard(false, settings, new NRTReplicationEngineFactory());
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(primaryShard, replicaShard);
        super.tearDown();
    }

    public void testGetCheckpointMetadata() throws ExecutionException, InterruptedException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
        CheckpointInfoResponse response = res.get();
        assert (response.getCheckpoint().equals(checkpoint));
        assert (response.getMetadataMap().isEmpty() == false);
    }

    public void testGetCheckpointMetadataFailure() {
        IndexShard mockShard = mock(IndexShard.class);
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        when(mockShard.getSegmentInfosSnapshot()).thenThrow(new RuntimeException("test"));
        assertThrows(RuntimeException.class, () -> {
            replicationSource = new RemoteStoreReplicationSource(mockShard);
            final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
            replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
            res.get();
        });
    }

    public void testGetSegmentFiles() throws ExecutionException, InterruptedException, IOException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        List<StoreFileMetadata> filesToFetch = primaryShard.getSegmentMetadataMap().values().stream().collect(Collectors.toList());
        final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, filesToFetch, replicaShard, res);
        GetSegmentFilesResponse response = res.get();
        assertEquals(response.files.size(), filesToFetch.size());
        assertTrue(response.files.containsAll(filesToFetch));
        closeShards(replicaShard);
    }

    public void testGetSegmentFilesAlreadyExists() throws IOException, InterruptedException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        List<StoreFileMetadata> filesToFetch = primaryShard.getSegmentMetadataMap().values().stream().collect(Collectors.toList());
        CountDownLatch latch = new CountDownLatch(1);
        try {
            final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
            replicationSource = new RemoteStoreReplicationSource(primaryShard);
            replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, filesToFetch, primaryShard, res);
            res.get();
        } catch (AssertionError | ExecutionException ex) {
            latch.countDown();
            assertTrue(ex instanceof AssertionError);
            assertTrue(ex.getMessage().startsWith("Local store already contains the file"));
        }
        latch.await();
    }

    public void testGetSegmentFilesReturnEmptyResponse() throws ExecutionException, InterruptedException {
        final ReplicationCheckpoint checkpoint = primaryShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
        replicationSource = new RemoteStoreReplicationSource(primaryShard);
        replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, Collections.emptyList(), primaryShard, res);
        GetSegmentFilesResponse response = res.get();
        assert (response.files.isEmpty());
    }

    public void testGetCheckpointMetadataEmpty() throws ExecutionException, InterruptedException, IOException {
        IndexShard mockShard = mock(IndexShard.class);
        // Build mockShard to return replicaShard directory so that empty metadata file is returned.
        buildIndexShardBehavior(mockShard, replicaShard);
        replicationSource = new RemoteStoreReplicationSource(mockShard);

        // Mock replica shard state to RECOVERING so that getCheckpointInfo return empty map
        final ReplicationCheckpoint checkpoint = replicaShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
        when(mockShard.state()).thenReturn(IndexShardState.RECOVERING);
        replicationSource = new RemoteStoreReplicationSource(mockShard);
        // Recovering shard should just do a noop and return empty metadata map.
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
        CheckpointInfoResponse response = res.get();
        assert (response.getCheckpoint().equals(checkpoint));
        assert (response.getMetadataMap().isEmpty());

        // Started shard should fail with assertion error.
        when(mockShard.state()).thenReturn(IndexShardState.STARTED);
        expectThrows(AssertionError.class, () -> {
            final PlainActionFuture<CheckpointInfoResponse> res2 = PlainActionFuture.newFuture();
            replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res2);
        });
    }

    private void buildIndexShardBehavior(IndexShard mockShard, IndexShard indexShard) {
        when(mockShard.getSegmentInfosSnapshot()).thenReturn(indexShard.getSegmentInfosSnapshot());
        Store remoteStore = mock(Store.class);
        when(mockShard.remoteStore()).thenReturn(remoteStore);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate()).getDelegate();
        FilterDirectory remoteStoreFilterDirectory = new RemoteStoreRefreshListenerTests.TestFilterDirectory(new RemoteStoreRefreshListenerTests.TestFilterDirectory(remoteSegmentStoreDirectory));
        when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);
    }
}
