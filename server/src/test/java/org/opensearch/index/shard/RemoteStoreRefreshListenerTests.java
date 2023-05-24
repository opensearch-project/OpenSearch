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
import org.junit.After;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.remote.RemoteRefreshSegmentPressureService;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.index.shard.RemoteStoreRefreshListener.SEGMENT_INFO_SNAPSHOT_FILENAME_PREFIX;

public class RemoteStoreRefreshListenerTests extends IndexShardTestCase {
    private IndexShard indexShard;
    private ClusterService clusterService;
    private RemoteStoreRefreshListener remoteStoreRefreshListener;
    private RemoteRefreshSegmentPressureService remoteRefreshSegmentPressureService;

    public void setup(boolean primary, int numberOfDocs) throws IOException {
        indexShard = newStartedShard(
            primary,
            Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true).build(),
            new InternalEngineFactory()
        );

        indexDocs(1, numberOfDocs);
        indexShard.refresh("test");

        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        remoteRefreshSegmentPressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        remoteRefreshSegmentPressureService.afterIndexShardCreated(indexShard);
        remoteStoreRefreshListener = new RemoteStoreRefreshListener(
            indexShard,
            SegmentReplicationCheckpointPublisher.EMPTY,
            remoteRefreshSegmentPressureService.getRemoteRefreshSegmentTracker(indexShard.shardId())
        );
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

        for (int i = 0; i < RemoteStoreRefreshListener.LAST_N_METADATA_FILES_TO_KEEP + 3; i++) {
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
        // We spy on IndexShard.getReplicationTracker() to validate that we have tried running remote time as per the expectation.
        CountDownLatch refreshCountLatch = new CountDownLatch(succeedOnAttempt);
        // We spy on IndexShard.getEngine() to validate that we have successfully hit the terminal code for ascertaining successful upload.
        // Value has been set as 3 as during a successful upload IndexShard.getEngine() is hit thrice and with mockito we are counting down
        CountDownLatch successLatch = new CountDownLatch(3);
        Tuple<RemoteStoreRefreshListener, RemoteRefreshSegmentPressureService> tuple = mockIndexShardWithRetryAndScheduleRefresh(
            succeedOnAttempt,
            refreshCountLatch,
            successLatch
        );
        assertBusy(() -> assertEquals(0, refreshCountLatch.getCount()));
        assertBusy(() -> assertEquals(0, successLatch.getCount()));
        RemoteRefreshSegmentPressureService pressureService = tuple.v2();
        RemoteRefreshSegmentTracker segmentTracker = pressureService.getRemoteRefreshSegmentTracker(indexShard.shardId());
        assertEquals(0, segmentTracker.getBytesLag());
        assertEquals(0, segmentTracker.getRefreshSeqNoLag());
        assertEquals(0, segmentTracker.getTimeMsLag());
        assertEquals(0, segmentTracker.getTotalUploadsFailed());
    }

    public void testRefreshSuccessOnSecondAttempt() throws Exception {
        // This covers 2 cases - 1) isRetry=false, shouldRetry=true 2) isRetry=true, shouldRetry=false
        // Succeed on 2nd attempt
        int succeedOnAttempt = 2;
        // We spy on IndexShard.getReplicationTracker() to validate that we have tried running remote time as per the expectation.
        CountDownLatch refreshCountLatch = new CountDownLatch(succeedOnAttempt);
        // We spy on IndexShard.getEngine() to validate that we have successfully hit the terminal code for ascertaining successful upload.
        // Value has been set as 3 as during a successful upload IndexShard.getEngine() is hit thrice and with mockito we are counting down
        CountDownLatch successLatch = new CountDownLatch(3);
        Tuple<RemoteStoreRefreshListener, RemoteRefreshSegmentPressureService> tuple = mockIndexShardWithRetryAndScheduleRefresh(
            succeedOnAttempt,
            refreshCountLatch,
            successLatch
        );
        assertBusy(() -> assertEquals(0, refreshCountLatch.getCount()));
        assertBusy(() -> assertEquals(0, successLatch.getCount()));
        RemoteRefreshSegmentPressureService pressureService = tuple.v2();
        RemoteRefreshSegmentTracker segmentTracker = pressureService.getRemoteRefreshSegmentTracker(indexShard.shardId());
        assertEquals(0, segmentTracker.getBytesLag());
        assertEquals(0, segmentTracker.getRefreshSeqNoLag());
        assertEquals(0, segmentTracker.getTimeMsLag());
        assertEquals(1, segmentTracker.getTotalUploadsFailed());
    }

    public void testRefreshSuccessOnThirdAttemptAttempt() throws Exception {
        // This covers 3 cases - 1) isRetry=false, shouldRetry=true 2) isRetry=true, shouldRetry=false 3) isRetry=True, shouldRetry=true
        // Succeed on 3rd attempt
        int succeedOnAttempt = 3;
        // We spy on IndexShard.getReplicationTracker() to validate that we have tried running remote time as per the expectation.
        CountDownLatch refreshCountLatch = new CountDownLatch(succeedOnAttempt);
        // We spy on IndexShard.getEngine() to validate that we have successfully hit the terminal code for ascertaining successful upload.
        // Value has been set as 3 as during a successful upload IndexShard.getEngine() is hit thrice and with mockito we are counting down
        CountDownLatch successLatch = new CountDownLatch(3);
        Tuple<RemoteStoreRefreshListener, RemoteRefreshSegmentPressureService> tuple = mockIndexShardWithRetryAndScheduleRefresh(
            succeedOnAttempt,
            refreshCountLatch,
            successLatch
        );
        assertBusy(() -> assertEquals(0, refreshCountLatch.getCount()));
        assertBusy(() -> assertEquals(0, successLatch.getCount()));
        RemoteRefreshSegmentPressureService pressureService = tuple.v2();
        RemoteRefreshSegmentTracker segmentTracker = pressureService.getRemoteRefreshSegmentTracker(indexShard.shardId());
        assertEquals(0, segmentTracker.getBytesLag());
        assertEquals(0, segmentTracker.getRefreshSeqNoLag());
        assertEquals(0, segmentTracker.getTimeMsLag());
        assertEquals(2, segmentTracker.getTotalUploadsFailed());

    }

    public void testTrackerData() throws Exception {
        Tuple<RemoteStoreRefreshListener, RemoteRefreshSegmentPressureService> tuple = mockIndexShardWithRetryAndScheduleRefresh(1);
        RemoteStoreRefreshListener listener = tuple.v1();
        RemoteRefreshSegmentPressureService pressureService = tuple.v2();
        RemoteRefreshSegmentTracker tracker = pressureService.getRemoteRefreshSegmentTracker(indexShard.shardId());
        assertNoLag(tracker);
        indexDocs(100, randomIntBetween(100, 200));
        indexShard.refresh("test");
        listener.afterRefresh(true);
        assertBusy(() -> assertNoLag(tracker));
    }

    private void assertNoLag(RemoteRefreshSegmentTracker tracker) {
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

    private Tuple<RemoteStoreRefreshListener, RemoteRefreshSegmentPressureService> mockIndexShardWithRetryAndScheduleRefresh(
        int succeedOnAttempt
    ) throws IOException {
        return mockIndexShardWithRetryAndScheduleRefresh(succeedOnAttempt, null, null);
    }

    private Tuple<RemoteStoreRefreshListener, RemoteRefreshSegmentPressureService> mockIndexShardWithRetryAndScheduleRefresh(
        int succeedOnAttempt,
        CountDownLatch refreshCountLatch,
        CountDownLatch successLatch
    ) throws IOException {
        // Create index shard that we will be using to mock different methods in IndexShard for the unit test
        indexShard = newStartedShard(
            true,
            Settings.builder()
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build(),
            new InternalEngineFactory()
        );

        indexDocs(1, randomIntBetween(1, 100));

        // Mock indexShard.store().directory()
        IndexShard shard = mock(IndexShard.class);
        Store store = mock(Store.class);
        when(shard.store()).thenReturn(store);
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
        when(shard.getOperationPrimaryTerm()).thenReturn(indexShard.getOperationPrimaryTerm());

        // Mock indexShard.routingEntry().primary()
        when(shard.routingEntry()).thenReturn(indexShard.routingEntry());

        // Mock threadpool
        when(shard.getThreadPool()).thenReturn(threadPool);

        // Mock indexShard.getReplicationTracker().isPrimaryMode()

        doAnswer(invocation -> {
            if (Objects.nonNull(refreshCountLatch)) {
                refreshCountLatch.countDown();
            }
            return indexShard.getReplicationTracker();
        }).when(shard).getReplicationTracker();

        AtomicLong counter = new AtomicLong();
        // Mock indexShard.getSegmentInfosSnapshot()
        doAnswer(invocation -> {
            if (counter.incrementAndGet() <= succeedOnAttempt - 1) {
                throw new RuntimeException("Inducing failure in upload");
            }
            return indexShard.getSegmentInfosSnapshot();
        }).when(shard).getSegmentInfosSnapshot();

        doAnswer(invocation -> {
            if (Objects.nonNull(successLatch)) {
                successLatch.countDown();
            }
            return indexShard.getEngine();
        }).when(shard).getEngine();

        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        RemoteRefreshSegmentPressureService remoteRefreshSegmentPressureService = new RemoteRefreshSegmentPressureService(
            clusterService,
            Settings.EMPTY
        );
        when(shard.indexSettings()).thenReturn(indexShard.indexSettings());
        when(shard.shardId()).thenReturn(indexShard.shardId());
        remoteRefreshSegmentPressureService.afterIndexShardCreated(shard);
        RemoteStoreRefreshListener refreshListener = new RemoteStoreRefreshListener(
            shard,
            SegmentReplicationCheckpointPublisher.EMPTY,
            remoteRefreshSegmentPressureService.getRemoteRefreshSegmentTracker(indexShard.shardId())
        );
        refreshListener.afterRefresh(true);
        return Tuple.tuple(refreshListener, remoteRefreshSegmentPressureService);
    }

    private static class TestFilterDirectory extends FilterDirectory {

        /**
         * Sole constructor, typically called from sub-classes.
         *
         * @param in input directory
         */
        protected TestFilterDirectory(Directory in) {
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
        if (segmentsNFilename != null) {
            String commitGeneration = segmentsNFilename.substring((IndexFileNames.SEGMENTS + "_").length());
            assertTrue(
                uploadedSegments.keySet()
                    .stream()
                    .anyMatch(
                        s -> s.startsWith(
                            SEGMENT_INFO_SNAPSHOT_FILENAME_PREFIX + "__" + Long.parseLong(commitGeneration, Character.MAX_RADIX)
                        )
                    )
            );
        }
    }
}
