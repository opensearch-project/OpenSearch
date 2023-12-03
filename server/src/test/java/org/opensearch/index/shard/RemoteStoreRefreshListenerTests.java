/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.index.store.RemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH;
import static org.opensearch.test.RemoteStoreTestUtils.createMetadataFileBytes;
import static org.opensearch.test.RemoteStoreTestUtils.getDummyMetadata;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteStoreRefreshListenerTests extends IndexShardTestCase {
    private IndexShard indexShard;
    private ClusterService clusterService;
    private RemoteStoreRefreshListener remoteStoreRefreshListener;
    private RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    public void setup(boolean primary, int numberOfDocs) throws IOException {
        indexShard = newStartedShard(
            primary,
            Settings.builder()
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "temp-fs")
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "temp-fs")
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build(),
            new InternalEngineFactory()
        );

        if (primary) {
            indexDocs(1, numberOfDocs);
            indexShard.refresh("test");
        }

        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(clusterService, Settings.EMPTY);
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        RemoteSegmentTransferTracker tracker = remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId());
        remoteStoreRefreshListener = new RemoteStoreRefreshListener(indexShard, SegmentReplicationCheckpointPublisher.EMPTY, tracker);
    }

    private void indexDocs(int startDocId, int numberOfDocs) throws IOException {
        for (int i = startDocId; i < startDocId + numberOfDocs; i++) {
            indexDoc(indexShard, "_doc", Integer.toString(i));
        }
    }

    @After
    public void tearDown() throws Exception {
        Directory storeDirectory = ((FilterDirectory) ((FilterDirectory) indexShard.store().directory()).getDelegate()).getDelegate();
        ((BaseDirectoryWrapper) storeDirectory).setCheckIndexOnClose(false);
        closeShards(indexShard);
        super.tearDown();
    }

    public void testRemoteDirectoryInitThrowsException() throws IOException {
        // Methods used in the constructor of RemoteSegmentTrackerListener have been mocked to reproduce specific exceptions
        // to test the failure modes possible during construction of RemoteSegmentTrackerListener object.
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());

        // Mocking the IndexShard methods and dependent classes.
        ShardId shardId = new ShardId("index1", "_na_", 1);
        IndexShard shard = mock(IndexShard.class);
        Store store = mock(Store.class);
        Directory directory = mock(Directory.class);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(shard.store()).thenReturn(store);
        when(store.directory()).thenReturn(directory);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shardRouting.primary()).thenReturn(true);
        when(shard.getThreadPool()).thenReturn(mock(ThreadPool.class));

        // Mock the Store, Directory and RemoteSegmentStoreDirectory classes
        Store remoteStore = mock(Store.class);
        when(shard.remoteStore()).thenReturn(remoteStore);
        RemoteDirectory remoteMetadataDirectory = mock(RemoteDirectory.class);
        AtomicLong listFilesCounter = new AtomicLong();

        // Below we are trying to get the IOException thrown in the constructor of the RemoteSegmentStoreDirectory.
        doAnswer(invocation -> {
            if (listFilesCounter.incrementAndGet() <= 1) {
                return Collections.singletonList("dummy string");
            }
            throw new IOException();
        }).when(remoteMetadataDirectory)
            .listFilesByPrefixInLexicographicOrder(MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH);

        SegmentInfos segmentInfos;
        try (Store indexShardStore = indexShard.store()) {
            segmentInfos = indexShardStore.readLastCommittedSegmentsInfo();
        }

        when(remoteMetadataDirectory.getBlobStream(any())).thenAnswer(
            I -> createMetadataFileBytes(getDummyMetadata("_0", 1), indexShard.getLatestReplicationCheckpoint(), segmentInfos)
        );
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            mock(RemoteDirectory.class),
            remoteMetadataDirectory,
            mock(RemoteStoreLockManager.class),
            mock(ThreadPool.class),
            shardId
        );
        FilterDirectory remoteStoreFilterDirectory = new RemoteStoreRefreshListenerTests.TestFilterDirectory(
            new RemoteStoreRefreshListenerTests.TestFilterDirectory(remoteSegmentStoreDirectory)
        );
        when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);

        // Since the thrown IOException is caught in the constructor, ctor should be invoked successfully.
        new RemoteStoreRefreshListener(shard, SegmentReplicationCheckpointPublisher.EMPTY, mock(RemoteSegmentTransferTracker.class));

        // Validate that the stream of metadata file of remoteMetadataDirectory has been opened only once and the
        // listFilesByPrefixInLexicographicOrder has been called twice.
        verify(remoteMetadataDirectory, times(1)).getBlobStream(any());
        verify(remoteMetadataDirectory, times(2)).listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            METADATA_FILES_TO_FETCH
        );
    }

    public void testAfterRefresh() throws IOException {
        setup(true, 3);
        assertDocs(indexShard, "1", "2", "3");

        try (Store remoteStore = indexShard.remoteStore()) {
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
                (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) remoteStore.directory()).getDelegate()).getDelegate();

            verifyUploadedSegments(remoteSegmentStoreDirectory);

            // This is to check if reading data from remote segment store works as well.
            remoteSegmentStoreDirectory.init();

            verifyUploadedSegments(remoteSegmentStoreDirectory);
        }
    }

    public void testAfterCommit() throws IOException {
        setup(true, 3);
        assertDocs(indexShard, "1", "2", "3");
        flushShard(indexShard);

        try (Store remoteStore = indexShard.remoteStore()) {
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
                (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) remoteStore.directory()).getDelegate()).getDelegate();

            verifyUploadedSegments(remoteSegmentStoreDirectory);

            // This is to check if reading data from remote segment store works as well.
            remoteSegmentStoreDirectory.init();

            verifyUploadedSegments(remoteSegmentStoreDirectory);
        }
    }

    public void testRefreshAfterCommit() throws IOException {
        setup(true, 3);
        assertDocs(indexShard, "1", "2", "3");
        flushShard(indexShard);

        indexDocs(4, 4);
        indexShard.refresh("test");

        indexDocs(8, 4);
        indexShard.refresh("test");

        try (Store remoteStore = indexShard.remoteStore()) {
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
                (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) remoteStore.directory()).getDelegate()).getDelegate();

            verifyUploadedSegments(remoteSegmentStoreDirectory);

            // This is to check if reading data from remote segment store works as well.
            remoteSegmentStoreDirectory.init();

            verifyUploadedSegments(remoteSegmentStoreDirectory);
        }
    }

    public void testAfterMultipleCommits() throws IOException {
        setup(true, 3);
        assertDocs(indexShard, "1", "2", "3");

        for (int i = 0; i < indexShard.getRecoverySettings().getMinRemoteSegmentMetadataFiles() + 3; i++) {
            indexDocs(4 * (i + 1), 4);
            flushShard(indexShard);
        }

        try (Store remoteStore = indexShard.remoteStore()) {
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
                (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) remoteStore.directory()).getDelegate()).getDelegate();

            verifyUploadedSegments(remoteSegmentStoreDirectory);

            // This is to check if reading data from remote segment store works as well.
            remoteSegmentStoreDirectory.init();

            verifyUploadedSegments(remoteSegmentStoreDirectory);
        }
    }

    public void testReplica() throws IOException {
        setup(false, 3);
        remoteStoreRefreshListener.afterRefresh(true);

        try (Store remoteStore = indexShard.remoteStore()) {
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
                (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) remoteStore.directory()).getDelegate()).getDelegate();

            assertEquals(0, remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().size());
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9773")
    public void testReplicaPromotion() throws IOException, InterruptedException {
        setup(false, 3);
        remoteStoreRefreshListener.afterRefresh(true);

        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
            (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate())
                .getDelegate();

        assertEquals(0, remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().size());

        final ShardRouting replicaRouting = indexShard.routingEntry();
        promoteReplica(
            indexShard,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build()
        );

        // The following logic is referenced from IndexShardTests.testPrimaryFillsSeqNoGapsOnPromotion
        // ToDo: Add wait logic as part of promoteReplica()
        final CountDownLatch latch = new CountDownLatch(1);
        indexShard.acquirePrimaryOperationPermit(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                releasable.close();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }, ThreadPool.Names.GENERIC, "");

        latch.await();

        indexDocs(4, 4);
        indexShard.refresh("test");
        remoteStoreRefreshListener.afterRefresh(true);

        verifyUploadedSegments(remoteSegmentStoreDirectory);

        // This is to check if reading data from remote segment store works as well.
        remoteSegmentStoreDirectory.init();

        verifyUploadedSegments(remoteSegmentStoreDirectory);
    }

    public void testRefreshSuccessOnFirstAttempt() throws Exception {
        // This is the case of isRetry=false, shouldRetry=false
        // Succeed on 1st attempt
        int succeedOnAttempt = 1;
        // We spy on IndexShard.isPrimaryStarted() to validate that we have tried running remote time as per the expectation.
        CountDownLatch refreshCountLatch = new CountDownLatch(succeedOnAttempt);
        // We spy on IndexShard.getEngine() to validate that we have successfully hit the terminal code for ascertaining successful upload.
        // Value has been set as 3 as during a successful upload IndexShard.getEngine() is hit thrice and with mockito we are counting down
        CountDownLatch successLatch = new CountDownLatch(3);
        Tuple<RemoteStoreRefreshListener, RemoteStoreStatsTrackerFactory> tuple = mockIndexShardWithRetryAndScheduleRefresh(
            succeedOnAttempt,
            refreshCountLatch,
            successLatch
        );
        assertBusy(() -> assertEquals(0, refreshCountLatch.getCount()));
        assertBusy(() -> assertEquals(0, successLatch.getCount()));
        RemoteStoreStatsTrackerFactory trackerFactory = tuple.v2();
        RemoteSegmentTransferTracker segmentTracker = trackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId());
        assertNoLagAndTotalUploadsFailed(segmentTracker, 0);
    }

    public void testRefreshSuccessOnSecondAttempt() throws Exception {
        // This covers 2 cases - 1) isRetry=false, shouldRetry=true 2) isRetry=true, shouldRetry=false
        // Succeed on 2nd attempt
        int succeedOnAttempt = 2;
        // We spy on IndexShard.isPrimaryStarted() to validate that we have tried running remote time as per the expectation.
        CountDownLatch refreshCountLatch = new CountDownLatch(succeedOnAttempt);
        // We spy on IndexShard.getEngine() to validate that we have successfully hit the terminal code for ascertaining successful upload.
        // Value has been set as 3 as during a successful upload IndexShard.getEngine() is hit thrice and with mockito we are counting down
        CountDownLatch successLatch = new CountDownLatch(3);
        Tuple<RemoteStoreRefreshListener, RemoteStoreStatsTrackerFactory> tuple = mockIndexShardWithRetryAndScheduleRefresh(
            succeedOnAttempt,
            refreshCountLatch,
            successLatch
        );
        assertBusy(() -> assertEquals(0, refreshCountLatch.getCount()));
        assertBusy(() -> assertEquals(0, successLatch.getCount()));
        RemoteStoreStatsTrackerFactory trackerFactory = tuple.v2();
        RemoteSegmentTransferTracker segmentTracker = trackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId());
        assertNoLagAndTotalUploadsFailed(segmentTracker, 1);
    }

    /**
     * Tests retry flow after snapshot and metadata files have been uploaded to remote store in the failed attempt.
     * Snapshot and metadata files created in failed attempt should not break retry.
     */
    public void testRefreshSuccessAfterFailureInFirstAttemptAfterSnapshotAndMetadataUpload() throws Exception {
        int succeedOnAttempt = 1;
        int checkpointPublishSucceedOnAttempt = 2;
        // We spy on IndexShard.isPrimaryStarted() to validate that we have tried running remote time as per the expectation.
        CountDownLatch refreshCountLatch = new CountDownLatch(succeedOnAttempt);
        // We spy on IndexShard.getEngine() to validate that we have successfully hit the terminal code for ascertaining successful upload.
        // Value has been set as 6 as during a successful upload IndexShard.getEngine() is hit thrice and here we are running the flow twice
        CountDownLatch successLatch = new CountDownLatch(3);
        CountDownLatch reachedCheckpointPublishLatch = new CountDownLatch(0);
        mockIndexShardWithRetryAndScheduleRefresh(
            succeedOnAttempt,
            refreshCountLatch,
            successLatch,
            checkpointPublishSucceedOnAttempt,
            reachedCheckpointPublishLatch
        );
        assertBusy(() -> assertEquals(0, refreshCountLatch.getCount()));
        assertBusy(() -> assertEquals(0, successLatch.getCount()));
        assertBusy(() -> assertEquals(0, reachedCheckpointPublishLatch.getCount()));
    }

    public void testRefreshSuccessOnThirdAttempt() throws Exception {
        // This covers 3 cases - 1) isRetry=false, shouldRetry=true 2) isRetry=true, shouldRetry=false 3) isRetry=True, shouldRetry=true
        // Succeed on 3rd attempt
        int succeedOnAttempt = 3;
        // We spy on IndexShard.isPrimaryStarted() to validate that we have tried running remote time as per the expectation.
        CountDownLatch refreshCountLatch = new CountDownLatch(succeedOnAttempt);
        // We spy on IndexShard.getEngine() to validate that we have successfully hit the terminal code for ascertaining successful upload.
        // Value has been set as 3 as during a successful upload IndexShard.getEngine() is hit thrice and with mockito we are counting down
        CountDownLatch successLatch = new CountDownLatch(3);
        Tuple<RemoteStoreRefreshListener, RemoteStoreStatsTrackerFactory> tuple = mockIndexShardWithRetryAndScheduleRefresh(
            succeedOnAttempt,
            refreshCountLatch,
            successLatch
        );
        assertBusy(() -> assertEquals(0, refreshCountLatch.getCount()));
        assertBusy(() -> assertEquals(0, successLatch.getCount()));
        RemoteStoreStatsTrackerFactory trackerFactory = tuple.v2();
        RemoteSegmentTransferTracker segmentTracker = trackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId());
        assertNoLagAndTotalUploadsFailed(segmentTracker, 2);
    }

    private void assertNoLagAndTotalUploadsFailed(RemoteSegmentTransferTracker segmentTracker, long totalUploadsFailed) throws Exception {
        assertBusy(() -> {
            assertEquals(0, segmentTracker.getBytesLag());
            assertEquals(0, segmentTracker.getRefreshSeqNoLag());
            assertEquals(0, segmentTracker.getTimeMsLag());
            assertEquals(totalUploadsFailed, segmentTracker.getTotalUploadsFailed());
        });
    }

    public void testTrackerData() throws Exception {
        Tuple<RemoteStoreRefreshListener, RemoteStoreStatsTrackerFactory> tuple = mockIndexShardWithRetryAndScheduleRefresh(1);
        RemoteStoreRefreshListener listener = tuple.v1();
        RemoteStoreStatsTrackerFactory trackerFactory = tuple.v2();
        RemoteSegmentTransferTracker tracker = trackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId());
        assertNoLag(tracker);
        indexDocs(100, randomIntBetween(100, 200));
        indexShard.refresh("test");
        listener.afterRefresh(true);
        assertBusy(() -> assertNoLag(tracker));
    }

    private void assertNoLag(RemoteSegmentTransferTracker tracker) {
        assertEquals(0, tracker.getRefreshSeqNoLag());
        assertEquals(0, tracker.getBytesLag());
        assertEquals(0, tracker.getTimeMsLag());
        assertEquals(0, tracker.getRejectionCount());
        assertEquals(tracker.getUploadBytesStarted(), tracker.getUploadBytesSucceeded());
        assertTrue(tracker.getUploadBytesStarted() > 0);
        assertEquals(0, tracker.getUploadBytesFailed());
        assertEquals(0, tracker.getInflightUploads());
        assertEquals(tracker.getTotalUploadsStarted(), tracker.getTotalUploadsSucceeded());
        assertTrue(tracker.getTotalUploadsStarted() > 0);
        assertEquals(0, tracker.getTotalUploadsFailed());
    }

    private Tuple<RemoteStoreRefreshListener, RemoteStoreStatsTrackerFactory> mockIndexShardWithRetryAndScheduleRefresh(
        int succeedOnAttempt
    ) throws IOException {
        return mockIndexShardWithRetryAndScheduleRefresh(succeedOnAttempt, null, null);
    }

    private Tuple<RemoteStoreRefreshListener, RemoteStoreStatsTrackerFactory> mockIndexShardWithRetryAndScheduleRefresh(
        int succeedOnAttempt,
        CountDownLatch refreshCountLatch,
        CountDownLatch successLatch
    ) throws IOException {
        CountDownLatch noOpLatch = new CountDownLatch(0);
        return mockIndexShardWithRetryAndScheduleRefresh(succeedOnAttempt, refreshCountLatch, successLatch, 1, noOpLatch);
    }

    private Tuple<RemoteStoreRefreshListener, RemoteStoreStatsTrackerFactory> mockIndexShardWithRetryAndScheduleRefresh(
        int succeedOnAttempt,
        CountDownLatch refreshCountLatch,
        CountDownLatch successLatch,
        int succeedCheckpointPublishOnAttempt,
        CountDownLatch reachedCheckpointPublishLatch
    ) throws IOException {
        // Create index shard that we will be using to mock different methods in IndexShard for the unit test
        indexShard = newStartedShard(
            true,
            Settings.builder()
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "temp-fs")
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "temp-fs")
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build(),
            new InternalEngineFactory()
        );

        indexDocs(1, randomIntBetween(1, 100));

        // Mock indexShard.store().directory()
        IndexShard shard = mock(IndexShard.class);
        Store store = mock(Store.class);
        when(shard.store()).thenReturn(store);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        when(store.directory()).thenReturn(indexShard.store().directory());

        // Mock (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
        Store remoteStore = mock(Store.class);
        when(shard.remoteStore()).thenReturn(remoteStore);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
            (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate())
                .getDelegate();
        FilterDirectory remoteStoreFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(remoteSegmentStoreDirectory));
        when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);

        // Mock indexShard.getOperationPrimaryTerm()
        when(shard.getLatestReplicationCheckpoint()).thenReturn(indexShard.getLatestReplicationCheckpoint());

        // Mock indexShard.routingEntry().primary()
        when(shard.routingEntry()).thenReturn(indexShard.routingEntry());

        // Mock threadpool
        when(shard.getThreadPool()).thenReturn(threadPool);

        // Mock indexShard.getReplicationTracker().isPrimaryMode()
        doAnswer(invocation -> {
            if (Objects.nonNull(refreshCountLatch)) {
                refreshCountLatch.countDown();
            }
            return true;
        }).when(shard).isStartedPrimary();

        AtomicLong counter = new AtomicLong();
        // Mock indexShard.getSegmentInfosSnapshot()
        doAnswer(invocation -> {
            if (counter.incrementAndGet() <= succeedOnAttempt) {
                throw new RuntimeException("Inducing failure in upload");
            }
            return indexShard.getSegmentInfosSnapshot();
        }).when(shard).getSegmentInfosSnapshot();

        doAnswer((invocation -> {
            if (counter.incrementAndGet() <= succeedOnAttempt) {
                throw new RuntimeException("Inducing failure in upload");
            }
            return indexShard.getLatestReplicationCheckpoint();
        })).when(shard).computeReplicationCheckpoint(any());

        doAnswer(invocation -> {
            if (Objects.nonNull(successLatch)) {
                successLatch.countDown();
            }
            return indexShard.getEngine();
        }).when(shard).getEngine();

        SegmentReplicationCheckpointPublisher emptyCheckpointPublisher = spy(SegmentReplicationCheckpointPublisher.EMPTY);
        AtomicLong checkpointPublisherCounter = new AtomicLong();
        doAnswer(invocation -> {
            if (checkpointPublisherCounter.incrementAndGet() <= succeedCheckpointPublishOnAttempt - 1) {
                throw new RuntimeException("Inducing failure after snapshot info snapshot to test if snapshot info file is deleted");
            }
            if (Objects.nonNull(reachedCheckpointPublishLatch)) {
                reachedCheckpointPublishLatch.countDown();
            }
            return null;
        }).when(emptyCheckpointPublisher).publish(any(), any());

        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory = indexShard.getRemoteStoreStatsTrackerFactory();
        when(shard.indexSettings()).thenReturn(indexShard.indexSettings());
        when(shard.shardId()).thenReturn(indexShard.shardId());
        RecoverySettings recoverySettings = mock(RecoverySettings.class);
        when(recoverySettings.getMinRemoteSegmentMetadataFiles()).thenReturn(10);
        when(shard.getRecoverySettings()).thenReturn(recoverySettings);
        RemoteSegmentTransferTracker tracker = remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId());
        RemoteStoreRefreshListener refreshListener = new RemoteStoreRefreshListener(shard, emptyCheckpointPublisher, tracker);
        refreshListener.afterRefresh(true);
        return Tuple.tuple(refreshListener, remoteStoreStatsTrackerFactory);
    }

    public static class TestFilterDirectory extends FilterDirectory {

        /**
         * Sole constructor, typically called from sub-classes.
         *
         * @param in input directory
         */
        public TestFilterDirectory(Directory in) {
            super(in);
        }
    }

    private void verifyUploadedSegments(RemoteSegmentStoreDirectory remoteSegmentStoreDirectory) throws IOException {
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();
        String segmentsNFilename = null;
        try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
            SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();
            for (String file : segmentInfos.files(true)) {
                if (!RemoteStoreRefreshListener.EXCLUDE_FILES.contains(file)) {
                    assertTrue(uploadedSegments.containsKey(file));
                }
                if (file.startsWith(IndexFileNames.SEGMENTS)) {
                    segmentsNFilename = file;
                }
            }
        }
    }

}
