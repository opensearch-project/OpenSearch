/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;
import org.opensearch.common.collect.Map;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CopyStateTests extends IndexShardTestCase {

    public void testCopyStateCreation() throws IOException {
        // dummy objects setup
        final long expectedLongValue = 1L;
        final ShardId testShardId = new ShardId("testIndex", "testUUID", 0);
        final StoreFileMetadata segmentsFile = new StoreFileMetadata(IndexFileNames.SEGMENTS, 1L, "0", Version.LATEST);
        final StoreFileMetadata pendingDeleteFile = new StoreFileMetadata("pendingDelete.del", 1L, "1", Version.LATEST);
        final Store.MetadataSnapshot commitMetadataSnapshot = new Store.MetadataSnapshot(
            Map.of("segmentsFile", segmentsFile, "pendingDeleteFile", pendingDeleteFile),
            null,
            0
        );
        final Store.MetadataSnapshot segmentInfosMetadataSnapshot = new Store.MetadataSnapshot(
            Map.of("segmentsFile", segmentsFile),
            null,
            0
        );

        // Mock objects setup
        IndexShard mockShard = mock(IndexShard.class);
        when(mockShard.shardId()).thenReturn(testShardId);
        when(mockShard.getOperationPrimaryTerm()).thenReturn(expectedLongValue);
        when(mockShard.getProcessedLocalCheckpoint()).thenReturn(expectedLongValue);

        Store mockStore = mock(Store.class);
        when(mockShard.store()).thenReturn(mockStore);

        SegmentInfos testSegmentInfos = new SegmentInfos(Version.LATEST.major);
        when(mockShard.getSegmentInfosSnapshot()).thenReturn(new GatedCloseable<>(testSegmentInfos, () -> {}));

        when(mockStore.getMetadata(testSegmentInfos)).thenReturn(segmentInfosMetadataSnapshot);

        IndexCommit mockIndexCommit = mock(IndexCommit.class);
        when(mockShard.acquireLastIndexCommit(false)).thenReturn(new GatedCloseable<>(mockIndexCommit, () -> {}));
        when(mockStore.getMetadata(mockIndexCommit)).thenReturn(commitMetadataSnapshot);

        // unit test
        CopyState copyState = new CopyState(mockShard);
        ReplicationCheckpoint checkpoint = copyState.getCheckpoint();
        assertEquals(testShardId, checkpoint.getShardId());
        // version was never set so this should be zero
        assertEquals(0, checkpoint.getSegmentInfosVersion());
        assertEquals(expectedLongValue, checkpoint.getPrimaryTerm());

        Set<StoreFileMetadata> pendingDeleteFiles = copyState.getPendingDeleteFiles();
        assertEquals(1, pendingDeleteFiles.size());
        assertTrue(pendingDeleteFiles.contains(pendingDeleteFile));
    }
}
