/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.Version;
import org.junit.Assert;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SegmentReplicationTargetTests extends IndexShardTestCase {

    SegmentReplicationTarget segrepTarget;
    IndexShard indexShard, spyIndexShard;
    ReplicationCheckpoint repCheckpoint;
    ByteBuffersDataOutput buffer;

    private static final StoreFileMetadata SEGMENTS_FILE = new StoreFileMetadata(IndexFileNames.SEGMENTS, 1L, "0", Version.LATEST);
    private static final StoreFileMetadata PENDING_DELETE_FILE = new StoreFileMetadata("pendingDelete.del", 1L, "1", Version.LATEST);

    private static final Store.MetadataSnapshot SI_SNAPSHOT = new Store.MetadataSnapshot(
        Map.of(SEGMENTS_FILE.name(), SEGMENTS_FILE),
        null,
        0
    );

    SegmentInfos testSegmentInfos = new SegmentInfos(Version.LATEST.major);

    @Override
    public void setUp() throws Exception {

        super.setUp();
        buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput indexOutput = new ByteBuffersIndexOutput(buffer, "", null)) {
            testSegmentInfos.write(indexOutput);
        }

        repCheckpoint = mock(ReplicationCheckpoint.class);
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());
        spyIndexShard = spy(indexShard);
        Mockito.doNothing().when(spyIndexShard).finalizeReplication(any(SegmentInfos.class), any(), anyLong());
    }

    public void testSuccessfulResponse_startReplication() {

        SegmentReplicationSource segrepSource = new SegmentReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy(), Set.of(PENDING_DELETE_FILE)));
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                Store store,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        };
        SegmentReplicationTargetService.SegmentReplicationListener segRepListener = mock(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        segrepTarget = new SegmentReplicationTarget(repCheckpoint, spyIndexShard, segrepSource, segRepListener);

        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                try {
                    verify(spyIndexShard, times(1)).finalizeReplication(any(), any(), anyLong());
                } catch (IOException ex) {
                    Assert.fail();
                }
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail();
            }
        });
    }

    public void testFailureResponse_getCheckpointMetadata() {
        Exception exception = new Exception("dummy failure");
        SegmentReplicationSource segrepSource = new SegmentReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onFailure(exception);
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                Store store,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        };
        SegmentReplicationTargetService.SegmentReplicationListener segRepListener = mock(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        segrepTarget = new SegmentReplicationTarget(repCheckpoint, spyIndexShard, segrepSource, segRepListener);

        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e.getCause().getCause());
            }
        });
    }

    public void testFailureResponse_getSegmentFiles() {
        Exception exception = new Exception("dummy failure");
        SegmentReplicationSource segrepSource = new SegmentReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy(), Set.of(PENDING_DELETE_FILE)));
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                Store store,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                listener.onFailure(exception);
            }
        };
        SegmentReplicationTargetService.SegmentReplicationListener segRepListener = mock(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        segrepTarget = new SegmentReplicationTarget(repCheckpoint, spyIndexShard, segrepSource, segRepListener);

        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e.getCause().getCause());
            }
        });
    }

    public void testFailure_finalizeReplication() throws IOException {
        IOException exception = new IOException("dummy failure");
        SegmentReplicationSource segrepSource = new SegmentReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy(), Set.of(PENDING_DELETE_FILE)));
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                Store store,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        };
        SegmentReplicationTargetService.SegmentReplicationListener segRepListener = mock(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        segrepTarget = new SegmentReplicationTarget(repCheckpoint, spyIndexShard, segrepSource, segRepListener);

        doThrow(exception).when(spyIndexShard).finalizeReplication(any(), any(), anyLong());

        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e.getCause());
            }
        });
    }

    @Override
    public void tearDown() throws Exception {
        logger.info(indexShard.store());
        super.tearDown();
        segrepTarget.markAsDone();
        closeShards(spyIndexShard, indexShard);
    }
}
