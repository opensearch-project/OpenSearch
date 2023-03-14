/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;
import org.junit.Assert;
import org.mockito.Mockito;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.StoreTests;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentReplicationTargetTests extends IndexShardTestCase {

    private SegmentReplicationTarget segrepTarget;
    private IndexShard indexShard, spyIndexShard;
    private ReplicationCheckpoint repCheckpoint;
    private ByteBuffersDataOutput buffer;

    private static final String SEGMENT_NAME = "_0.si";
    private static final StoreFileMetadata SEGMENT_FILE = new StoreFileMetadata(SEGMENT_NAME, 1L, "0", Version.LATEST);
    private static final StoreFileMetadata SEGMENT_FILE_DIFF = new StoreFileMetadata(SEGMENT_NAME, 5L, "different", Version.LATEST);

    private static final Map<String, StoreFileMetadata> SI_SNAPSHOT = Map.of(SEGMENT_FILE.name(), SEGMENT_FILE);

    private static final Map<String, StoreFileMetadata> SI_SNAPSHOT_DIFFERENT = Map.of(SEGMENT_FILE_DIFF.name(), SEGMENT_FILE_DIFF);

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
    );

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

        Mockito.doNothing().when(spyIndexShard).finalizeReplication(any(SegmentInfos.class));
        testSegmentInfos = spyIndexShard.store().readLastCommittedSegmentsInfo();
        buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput indexOutput = new ByteBuffersIndexOutput(buffer, "", null)) {
            testSegmentInfos.write(indexOutput);
        }
        repCheckpoint = new ReplicationCheckpoint(
            spyIndexShard.shardId(),
            spyIndexShard.getPendingPrimaryTerm(),
            testSegmentInfos.getGeneration(),
            testSegmentInfos.version
        );
    }

    public void testSuccessfulResponse_startReplication() {

        SegmentReplicationSource segrepSource = new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy()));
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                Store store,
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
        segrepTarget = new SegmentReplicationTarget(repCheckpoint, spyIndexShard, segrepSource, segRepListener);

        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                try {
                    verify(spyIndexShard, times(1)).finalizeReplication(any());
                    segrepTarget.markAsDone();
                } catch (IOException ex) {
                    Assert.fail();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected onFailure", e);
                Assert.fail();
            }
        });
    }

    public void testFailureResponse_getCheckpointMetadata() {

        Exception exception = new Exception("dummy failure");
        SegmentReplicationSource segrepSource = new TestReplicationSource() {
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
                segrepTarget.fail(new ReplicationFailedException(e), false);
            }
        });
    }

    public void testFailureResponse_getSegmentFiles() {

        Exception exception = new Exception("dummy failure");
        SegmentReplicationSource segrepSource = new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy()));
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
                segrepTarget.fail(new ReplicationFailedException(e), false);
            }
        });
    }

    public void testFailure_finalizeReplication_IOException() throws IOException {

        IOException exception = new IOException("dummy failure");
        SegmentReplicationSource segrepSource = new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy()));
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

        doThrow(exception).when(spyIndexShard).finalizeReplication(any());

        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e.getCause());
                segrepTarget.fail(new ReplicationFailedException(e), false);
            }
        });
    }

    public void testFailure_finalizeReplication_IndexFormatException() throws IOException {

        IndexFormatTooNewException exception = new IndexFormatTooNewException("string", 1, 2, 1);
        SegmentReplicationSource segrepSource = new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy()));
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

        doThrow(exception).when(spyIndexShard).finalizeReplication(any());

        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e.getCause());
                segrepTarget.fail(new ReplicationFailedException(e), false);
            }
        });
    }

    public void testFailure_differentSegmentFiles() throws IOException {

        SegmentReplicationSource segrepSource = new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, SI_SNAPSHOT, buffer.toArrayCopy()));
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
        when(spyIndexShard.getSegmentMetadataMap()).thenReturn(SI_SNAPSHOT_DIFFERENT);
        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                Assert.fail();
            }

            @Override
            public void onFailure(Exception e) {
                assert (e instanceof ReplicationFailedException);
                assert (e.getMessage().contains("different segment files"));
            }
        });
    }

    /**
     * This tests ensures that new files generated on primary (due to delete operation) are not considered missing on replica
     * @throws IOException if an indexing operation fails or segment replication fails
     */
    public void test_MissingFiles_NotCausingFailure() throws IOException {
        int docCount = 1 + random().nextInt(10);
        // Generate a list of MetadataSnapshot containing two elements. The second snapshot contains extra files
        // generated due to delete operations. These two snapshots can then be used in test to mock the primary shard
        // snapshot (2nd element which contains delete operations) and replica's existing snapshot (1st element).
        List<Store.MetadataSnapshot> storeMetadataSnapshots = generateStoreMetadataSnapshot(docCount);

        SegmentReplicationSource segrepSource = new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, storeMetadataSnapshots.get(1).asMap(), buffer.toArrayCopy()));
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
        when(spyIndexShard.getSegmentMetadataMap()).thenReturn(storeMetadataSnapshots.get(0).asMap());
        segrepTarget.startReplication(new ActionListener<Void>() {
            @Override
            public void onResponse(Void replicationResponse) {
                logger.info("No error processing checkpoint info");
                segrepTarget.markAsDone();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected onFailure", e);
                Assert.fail();
            }
        });
    }

    /**
     * Generates a list of Store.MetadataSnapshot with two elements where second snapshot has extra files due to delete
     * operation. A list of snapshots is returned so that identical files have same checksum.
     * @param docCount the number of documents to index in the first snapshot
     * @return a list of Store.MetadataSnapshot with two elements where second snapshot has extra files due to delete
     * @throws IOException if one of the indexing operations fails
     */
    private List<Store.MetadataSnapshot> generateStoreMetadataSnapshot(int docCount) throws IOException {
        List<Document> docList = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            Document document = new Document();
            String text = new String(new char[] { (char) (97 + i), (char) (97 + i) });
            document.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            document.add(new TextField("str", text, Field.Store.YES));
            docList.add(document);
        }
        long seed = random().nextLong();
        Random random = new Random(seed);
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setUseCompoundFile(true);
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        IndexWriter writer = new IndexWriter(store.directory(), iwc);
        for (Document d : docList) {
            writer.addDocument(d);
        }
        writer.commit();
        Store.MetadataSnapshot storeMetadata = store.getMetadata();
        // Delete one document to generate .liv file
        writer.deleteDocuments(new Term("id", Integer.toString(random().nextInt(docCount))));
        writer.commit();
        Store.MetadataSnapshot storeMetadataWithDeletes = store.getMetadata();
        deleteContent(store.directory());
        writer.close();
        store.close();
        return Arrays.asList(storeMetadata, storeMetadataWithDeletes);
    }

    private static void deleteContent(Directory directory) throws IOException {
        final String[] files = directory.listAll();
        final List<IOException> exceptions = new ArrayList<>();
        for (String file : files) {
            try {
                directory.deleteFile(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            } catch (IOException e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(spyIndexShard, indexShard);
    }

}
