/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.index.IndexService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Behaviour of the local translog a {@code segments_only} shard keeps. Nothing uploads it, so it has to behave the
 * way a document replication translog does rather than the way a remote backed one does: bounded by flush, synced
 * inline rather than through the remote upload buffer, usable as the source of a realtime read, and free to use the
 * durability settings a local translog supports.
 *
 * <p>What a translog does on disk can only be observed below the shard, so this class runs with a filesystem that
 * counts {@code force} calls on translog files and can refuse checkpoint writes to simulate a power cut. Both are
 * off unless a test turns them on, so the rest of these are unaffected by it beyond the wrapper itself.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class SegmentsOnlyTranslogIT extends SegmentsOnlyBaseIntegTestCase {

    private static final int SETTLED_TRANSLOG_OPS = 10;
    private static final String INDEX_NAME = "segments-only-translog-idx";

    private static TranslogFileSystemProvider fileSystemProvider;

    @BeforeClass
    public static void installTranslogFileSystem() {
        fileSystemProvider = new TranslogFileSystemProvider(PathUtils.getDefaultFileSystem());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeTranslogFileSystem() {
        PathUtilsForTesting.teardown();
        fileSystemProvider = null;
    }

    @After
    public void stopDisruptingTheTranslog() {
        fileSystemProvider.counting.set(false);
        fileSystemProvider.failCheckpointWrites.set(false);
    }

    /**
     * {@code IndexShard#trimTranslog} returns early on a remote node, so the only thing left to bound a
     * segments_only translog is the trim a flush performs, and that needs the persisted global checkpoint to keep
     * advancing. Reverting the {@code GlobalCheckpointSyncAction} change leaves the shard holding every operation.
     */
    public void testTranslogIsBoundedWhileIdle() throws Exception {
        String dataNode = startCluster(1).get(0);
        createSegmentsOnlyIndex(
            INDEX_NAME,
            0,
            Settings.builder()
                .put("index.translog.flush_threshold_size", "256b")
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );

        indexDocs(INDEX_NAME, 0, 200);
        IndexShard shard = shardOn(dataNode, INDEX_NAME);

        // Writes have stopped. A document replication shard settles back to a single generation holding only the
        // operations above the global checkpoint, so anything materially larger here is translog that is never freed.
        assertBusy(() -> {
            long ops = shard.translogStats().estimatedNumberOfOperations();
            long bytes = shard.translogStats().getTranslogSizeInBytes();
            // A settled shard keeps only the operations above the global checkpoint, plus the odd in-flight sync.
            assertTrue("translog still holds " + ops + " ops / " + bytes + " bytes after settling", ops <= SETTLED_TRANSLOG_OPS);
        });
    }

    /** Async translog durability, which is a local translog durability choice that document replication allows. */
    public void testAsyncTranslogDurabilityIsAllowed() throws Exception {
        String dataNode = startCluster(1).get(0);
        createSegmentsOnlyIndex(INDEX_NAME, 0, Settings.builder().put("index.translog.durability", "async").build());

        IndexShard shard = shardOn(dataNode, INDEX_NAME);
        assertSegmentsOnly(shard);
        assertEquals(Translog.Durability.ASYNC, shard.indexSettings().getTranslogDurability());

        int docs = indexDocs(INDEX_NAME, 0, 20);
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs);
    }

    /**
     * A realtime get before any refresh is served out of the translog, so it is the one read path that depends on
     * the local translog being readable.
     */
    public void testRealtimeGetIsServedFromTheLocalTranslog() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        indexDocs(INDEX_NAME, 0, 10);
        // No refresh, so nothing is searchable yet and only the translog can answer.
        for (int i = 0; i < 10; i++) {
            assertTrue("realtime get must find doc " + i, client().prepareGet(INDEX_NAME, Integer.toString(i)).get().isExists());
        }
    }

    /**
     * The buffered translog processor defers each fsync onto the {@code translog_sync} pool so that remote translog
     * uploads can be batched. A segments_only translog is never uploaded, so that deferral buys nothing and only
     * delays the write response by the upload interval. Writes here must sync inline instead, leaving the pool idle.
     */
    public void testLocalTranslogSyncedWithoutUploadBuffer() throws Exception {
        String dataNode = startCluster(1).get(0);
        createSegmentsOnlyIndex(INDEX_NAME, 0);

        IndexShard shard = internalCluster().getInstance(IndicesService.class, dataNode).indexService(resolveIndex(INDEX_NAME)).getShard(0);
        assertSegmentsOnly(shard);

        // Writes are issued one at a time with the default REQUEST durability, so each one is separately fsynced and
        // would separately schedule onto the buffer pool.
        int docs = randomIntBetween(10, 20);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs);

        assertFalse(
            "a local translog must be synced inline, not deferred onto the remote upload buffer",
            shard.getTranslogSyncProcessor() instanceof BufferedAsyncIOProcessor
        );
    }

    /**
     * Watches the translog files below the shard. It counts {@code force} calls, which is the syscall that makes an
     * operation survive the machine, and on demand refuses to open a checkpoint file at all, which leaves the
     * checkpoint stuck where it was and is how a power cut looks from above.
     */
    static class TranslogFileSystemProvider extends FilterFileSystemProvider {

        final AtomicBoolean counting = new AtomicBoolean();
        final AtomicReference<String> countingForIndexUuid = new AtomicReference<>();
        final AtomicBoolean failCheckpointWrites = new AtomicBoolean();
        final AtomicInteger translogFsyncs = new AtomicInteger();
        final AtomicInteger checkpointFsyncs = new AtomicInteger();
        final AtomicInteger refusedCheckpointWrites = new AtomicInteger();

        TranslogFileSystemProvider(FileSystem inner) {
            super("segmentsonlytranslog://", inner);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            String name = path.getFileName().toString();
            if (failCheckpointWrites.get() && name.endsWith(".ckp")) {
                refusedCheckpointWrites.incrementAndGet();
                throw new IOException("fake IOException");
            }
            FileChannel delegate = super.newFileChannel(path, options, attrs);
            if (name.endsWith(".tlog") == false && name.endsWith(".ckp") == false) {
                return delegate;
            }
            boolean translog = name.endsWith(".tlog");
            return new FilterFileChannel(delegate) {
                @Override
                public void force(boolean metaData) throws IOException {
                    String indexUuid = countingForIndexUuid.get();
                    if (counting.get() && indexUuid != null && path.toString().contains(indexUuid)) {
                        (translog ? translogFsyncs : checkpointFsyncs).incrementAndGet();
                    }
                    super.force(metaData);
                }
            };
        }
    }

    public void testLocalTranslogFsyncedBeforeAcknowledgement() throws Exception {
        startCluster(1);

        createSegmentsOnlyIndex(INDEX_NAME, 0);

        // Count only this index's writes, so that neither shard startup nor another index can contribute.
        fileSystemProvider.countingForIndexUuid.set(resolveIndex(INDEX_NAME).getUUID());
        fileSystemProvider.translogFsyncs.set(0);
        fileSystemProvider.checkpointFsyncs.set(0);
        fileSystemProvider.counting.set(true);
        int docs = randomIntBetween(10, 20);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        fileSystemProvider.counting.set(false);

        logger.info(
            "--> segments_only writes: {} translog fsyncs, {} checkpoint fsyncs over {} docs",
            fileSystemProvider.translogFsyncs.get(),
            fileSystemProvider.checkpointFsyncs.get(),
            docs
        );

        assertThat(
            "a segments_only translog is the only durable copy and must be fsynced",
            fileSystemProvider.translogFsyncs.get(),
            greaterThan(0)
        );
        assertThat("the translog checkpoint must be fsynced alongside it", fileSystemProvider.checkpointFsyncs.get(), greaterThan(0));
    }

    /**
     * After a power cut, every operation at or below the global checkpoint the cluster advertised is still there.
     * This shard has to reconcile two sources to get it: segments that reached the remote store before the crash,
     * and a local translog torn off at the last surviving checkpoint. Modelled on
     * {@code DiskDisruptionIT#testGlobalCheckpointIsSafe}, where refusing checkpoint writes loses exactly the
     * un-fsynced tail.
     */
    public void testOperationsBelowGlobalCheckpointSurvivePowerCut() throws Exception {
        String dataNode = startCluster(1).get(0);
        // A translog writer opens its checkpoint file once and holds it, so refusing to open one only bites when a
        // generation rolls. Rolling often makes the crash land during the window below rather than by luck.
        createSegmentsOnlyIndex(INDEX_NAME, 0, Settings.builder().put("index.translog.generation_threshold_size", "1kb").build());

        IndexShard shard = shardOn(dataNode, INDEX_NAME);
        assertSegmentsOnly(shard);

        // Sample the global checkpoint the cluster advertises while indexing runs. Whatever it reaches is what the
        // shard has promised, and the promise has to hold across the crash.
        AtomicBoolean stopSampling = new AtomicBoolean();
        AtomicLong highestGlobalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        Thread sampler = new Thread(() -> {
            while (stopSampling.get() == false) {
                try {
                    for (ShardStats stats : client().admin().indices().prepareStats(INDEX_NAME).clear().get().getShards()) {
                        long gcp = stats.getSeqNoStats().getGlobalCheckpoint();
                        highestGlobalCheckpoint.getAndUpdate(current -> Math.max(current, gcp));
                    }
                } catch (Exception e) {
                    logger.debug("failed to fetch shard stats", e);
                }
            }
        });
        sampler.start();

        try {
            try (
                BackgroundIndexer indexer = new BackgroundIndexer(
                    INDEX_NAME,
                    "_doc",
                    client(),
                    -1,
                    scaledRandomIntBetween(2, 5),
                    false,
                    random()
                )
            ) {
                indexer.setRequestTimeout(TimeValue.ZERO);
                indexer.setIgnoreIndexingFailures(true);
                indexer.setFailureAssertion(e -> {});
                indexer.start(-1);
                waitForDocs(randomIntBetween(1, 100), indexer);
                fileSystemProvider.refusedCheckpointWrites.set(0);
                fileSystemProvider.failCheckpointWrites.set(true);
                // Keep indexing until the disk has actually started refusing, so that the crash is something that
                // happened rather than a race the indexer won by stopping first.
                assertBusy(() -> assertThat(fileSystemProvider.refusedCheckpointWrites.get(), greaterThan(0)));
            }

            internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
                @Override
                public void onAllNodesStopped() {
                    // The machines are down, so the disk works again.
                    fileSystemProvider.failCheckpointWrites.set(false);
                }
            });
        } finally {
            stopSampling.set(true);
            sampler.join();
        }
        ensureGreen(INDEX_NAME);

        // Without this the comparison below holds against a checkpoint that never moved, and the test passes
        // whatever the crash did.
        assertThat(
            "the cluster never advertised a global checkpoint, so there is no promise to check",
            highestGlobalCheckpoint.get(),
            greaterThan(SequenceNumbers.NO_OPS_PERFORMED)
        );
        logger.info(
            "--> segments_only power cut: {} refused checkpoint writes, highest advertised global checkpoint {}",
            fileSystemProvider.refusedCheckpointWrites.get(),
            highestGlobalCheckpoint.get()
        );

        for (ShardStats stats : client().admin().indices().prepareStats(INDEX_NAME).clear().get().getShards()) {
            if (stats.getShardRouting().active()) {
                assertThat(
                    "operations at or below the advertised global checkpoint were lost in the crash",
                    stats.getSeqNoStats().getMaxSeqNo(),
                    greaterThanOrEqualTo(highestGlobalCheckpoint.get())
                );
            }
        }

        // The shard came back as what it was, rather than quietly falling back to a mode where the assertion above
        // would be met by something other than the local translog.
        IndexShard recovered = shardOn(internalCluster().getDataNodeNames().iterator().next(), INDEX_NAME);
        assertTrue("the recovered shard must still be remote segment backed", recovered.indexSettings().isRemoteStoreEnabled());
        assertFalse("the recovered shard must still keep its translog locally", recovered.indexSettings().isTranslogDurableInRemoteStore());
    }
}
