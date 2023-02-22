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
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.index.shard.RemoteStoreRefreshListener.SEGMENT_INFO_SNAPSHOT_FILENAME_PREFIX;

public class RemoteStoreRefreshListenerTests extends IndexShardTestCase {
    private IndexShard indexShard;
    private RemoteStoreRefreshListener remoteStoreRefreshListener;

    public void setup(boolean primary, int numberOfDocs) throws IOException {
        indexShard = newStartedShard(
            primary,
            Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true).build(),
            new InternalEngineFactory()
        );

        indexDocs(1, numberOfDocs);
        indexShard.refresh("test");

        remoteStoreRefreshListener = new RemoteStoreRefreshListener(indexShard);
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
