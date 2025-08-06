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
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.MergedSegmentCheckpoint;
import org.opensearch.indices.replication.checkpoint.RemoteStoreMergedSegmentCheckpoint;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationType;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MergedSegmentReplicationTargetTests extends IndexShardTestCase {

    private MergedSegmentReplicationTarget mergedSegmentReplicationTarget;
    private IndexShard indexShard, spyIndexShard;
    private MergedSegmentCheckpoint mergedSegmentCheckpoint;
    private RemoteStoreMergedSegmentCheckpoint remoteStoreMergedSegmentCheckpoint;
    private MergedSegmentCheckpoint mergedSegment;
    private ByteBuffersDataOutput buffer;

    private static final String SEGMENT_NAME = "_0.si";
    private static final StoreFileMetadata SEGMENT_FILE = new StoreFileMetadata(SEGMENT_NAME, 1L, "0", Version.LATEST);
    private static final StoreFileMetadata SEGMENT_FILE_DIFF = new StoreFileMetadata(SEGMENT_NAME, 5L, "different", Version.LATEST);

    private static final Map<String, StoreFileMetadata> SI_SNAPSHOT = Map.of(SEGMENT_FILE.name(), SEGMENT_FILE);

    private static final Map<String, StoreFileMetadata> SI_SNAPSHOT_DIFFERENT = Map.of(SEGMENT_FILE_DIFF.name(), SEGMENT_FILE_DIFF);

    private SegmentInfos testSegmentInfos;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();

        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());
        spyIndexShard = spy(indexShard);

        testSegmentInfos = spyIndexShard.store().readLastCommittedSegmentsInfo();
        buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput indexOutput = new ByteBuffersIndexOutput(buffer, "", null)) {
            testSegmentInfos.write(indexOutput);
        }
        mergedSegmentCheckpoint = new MergedSegmentCheckpoint(
            spyIndexShard.shardId(),
            spyIndexShard.getPendingPrimaryTerm(),
            1,
            1,
            indexShard.getLatestReplicationCheckpoint().getCodec(),
            SI_SNAPSHOT,
            IndexFileNames.parseSegmentName(SEGMENT_NAME)
        );
        remoteStoreMergedSegmentCheckpoint = new RemoteStoreMergedSegmentCheckpoint(
            new MergedSegmentCheckpoint(
                spyIndexShard.shardId(),
                spyIndexShard.getPendingPrimaryTerm(),
                1,
                1,
                indexShard.getLatestReplicationCheckpoint().getCodec(),
                SI_SNAPSHOT,
                IndexFileNames.parseSegmentName(SEGMENT_NAME)
            ),
            Map.of(SEGMENT_NAME, SEGMENT_NAME + "__uuid")
        );
    }

    public void testFailureDifferentSegmentFiles_remoteStoreEnabled() throws IOException {
        testFailureDifferentSegmentFiles(remoteStoreMergedSegmentCheckpoint);
    }

    public void testFailureDifferentSegmentFiles_segRep() throws IOException {
        testFailureDifferentSegmentFiles(mergedSegmentCheckpoint);
    }

    public void testFailureResponseGetMergedSegmentFiles_remoteStoreEnabled() {
        testFailureResponseGetMergedSegmentFiles(remoteStoreMergedSegmentCheckpoint);
    }

    public void testFailureResponseGetMergedSegmentFiles_segRep() {
        testFailureResponseGetMergedSegmentFiles(mergedSegmentCheckpoint);
    }

    public void testSuccessfulResponseStartReplication_remoteStoreEnabled() {
        testSuccessfulResponseStartReplication(remoteStoreMergedSegmentCheckpoint);
    }

    public void testSuccessfulResponseStartReplication_segRep() {
        testSuccessfulResponseStartReplication(mergedSegmentCheckpoint);
    }

    private void testSuccessfulResponseStartReplication(MergedSegmentCheckpoint checkpointMergedSegment) {

        SegmentReplicationSource segrepSource = new TestReplicationSource() {
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

            @Override
            public void getMergedSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                assertEquals(1, filesToFetch.size());
                assert (filesToFetch.contains(SEGMENT_FILE));
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        };

        SegmentReplicationTargetService.SegmentReplicationListener segRepListener = mock(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        mergedSegmentReplicationTarget = new MergedSegmentReplicationTarget(
            spyIndexShard,
            checkpointMergedSegment,
            segrepSource,
            segRepListener
        );

        mergedSegmentReplicationTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                mergedSegmentReplicationTarget.markAsDone();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected onFailure", e);
                Assert.fail();
            }
        }, (ReplicationCheckpoint checkpoint, IndexShard indexShard) -> {
            assertEquals(mergedSegmentCheckpoint, checkpoint);
            assertEquals(indexShard, spyIndexShard);
        });
    }

    private void testFailureResponseGetMergedSegmentFiles(MergedSegmentCheckpoint checkpointMergedSegment) {

        Exception exception = new Exception("dummy failure");
        SegmentReplicationSource segrepSource = new TestReplicationSource() {
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

            @Override
            public void getMergedSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                listener.onFailure(exception);
            }
        };
        SegmentReplicationTargetService.SegmentReplicationListener segRepListener = mock(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        mergedSegmentReplicationTarget = new MergedSegmentReplicationTarget(
            spyIndexShard,
            checkpointMergedSegment,
            segrepSource,
            segRepListener
        );

        mergedSegmentReplicationTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e.getCause().getCause());
                mergedSegmentReplicationTarget.fail(new ReplicationFailedException(e), false);
            }
        }, mock(BiConsumer.class));
    }

    private void testFailureDifferentSegmentFiles(MergedSegmentCheckpoint checkpointMergedSegment) throws IOException {

        SegmentReplicationSource segrepSource = new TestReplicationSource() {
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

            @Override
            public void getMergedSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        };
        SegmentReplicationTargetService.SegmentReplicationListener segRepListener = mock(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        mergedSegmentReplicationTarget = new MergedSegmentReplicationTarget(
            spyIndexShard,
            checkpointMergedSegment,
            segrepSource,
            segRepListener
        );
        when(spyIndexShard.getSegmentMetadataMap()).thenReturn(SI_SNAPSHOT_DIFFERENT);
        mergedSegmentReplicationTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof OpenSearchCorruptionException);
                assertTrue(e.getMessage().contains("has local copies of segments that differ from the primary"));
                mergedSegmentReplicationTarget.fail(new ReplicationFailedException(e), false);
            }
        }, mock(BiConsumer.class));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(spyIndexShard, indexShard);
    }
}
