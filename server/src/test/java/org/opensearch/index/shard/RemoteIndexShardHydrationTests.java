/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.engine.exec.IndexerFactory;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.mockstore.BlobStoreWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * A recovery of a remote-store shard hydrates segments and translog from the remote store before it opens the engine.
 * That hydration must behave like a peer recovery's phase 1: it runs outside the engine open critical section, so that
 * {@link IndexShard#close} - called from under the cluster applier's lock - is never held up by remote I/O, and closing
 * the shard aborts it (issue #15277).
 */
public class RemoteIndexShardHydrationTests extends IndexShardTestCase {

    /** Holds translog downloads when armed, and can reject any repository read after engine construction starts. */
    private static final class DownloadGate {
        volatile boolean armed;
        volatile boolean rejectReads;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch interrupted = new CountDownLatch(1);

        void pass(String blobName) throws IOException {
            if (rejectReads) {
                throw new IOException("remote read attempted during engine construction: " + blobName);
            }
            if (armed == false || blobName.endsWith(Translog.TRANSLOG_FILE_SUFFIX) == false) {
                return;
            }
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                // what a FileChannel write on the interrupted downloading thread would raise
                interrupted.countDown();
                Thread.currentThread().interrupt();
                throw new ClosedByInterruptException();
            }
        }
    }

    private final DownloadGate gate = new DownloadGate();

    @Override
    protected BlobStore wrapRemoteStoreBlobStore(BlobStore blobStore) {
        return new BlobStoreWrapper(blobStore) {
            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new FilterBlobContainer(super.blobContainer(path)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public InputStream readBlob(String blobName) throws IOException {
                        gate.pass(blobName);
                        return super.readBlob(blobName);
                    }

                    @Override
                    public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
                        gate.pass(blobName);
                        return super.readBlobWithMetadata(blobName);
                    }
                };
            }
        };
    }

    /**
     * While a recovery is blocked downloading the translog from the remote store, closing the shard must return promptly,
     * and the download must stop - the recovering thread exits with the shard reported closed and no engine was opened.
     * Before this change the download held engineMutex and close() parked on it for the whole download.
     */
    public void testCloseIsNotHeldUpByRemoteHydrationAndAbortsIt() throws Exception {
        String remoteStorePath = createTempDir().toString();
        IndexShard shard = newStartedShard(
            true,
            Settings.builder()
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, remoteStorePath + "__test")
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, remoteStorePath + "__test")
                .build(),
            new EngineBackedIndexerFactory(new InternalEngineFactory())
        );
        int numDocs = between(1, 5);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        // Not closeShard(): the harness refreshes on close, and a remote-store shard's refresh is an upload we do not
        // want racing the recovery below.
        IOUtils.close(() -> shard.close("test", false, false), shard.store());
        // A fresh node has no local translog, so every generation has to be fetched; with the local copy left in
        // place the incremental download would reuse it and never touch the remote store.
        IOUtils.rm(shard.shardPath().resolveTranslog());

        IndexShard recovering = reinitShard(shard);
        gate.armed = true;
        AtomicReference<Boolean> recovered = new AtomicReference<>();
        AtomicReference<Exception> recoveryFailure = new AtomicReference<>();
        Thread recovery = new Thread(() -> {
            try {
                recovering.markAsRecovering(
                    "store",
                    new RecoveryState(
                        recovering.routingEntry(),
                        IndexShardTestUtils.getFakeDiscoNode(recovering.routingEntry().currentNodeId()),
                        null
                    )
                );
                recovered.set(recoverFromStore(recovering));
            } catch (Exception e) {
                recoveryFailure.set(e);
            }
        }, "recovery");
        recovery.start();
        try {
            assertTrue("the recovery never reached the translog download", gate.entered.await(30, TimeUnit.SECONDS));
            assertThat(recovering.state(), equalTo(IndexShardState.RECOVERING));

            // The applier-side call: must not wait for the download.
            AtomicReference<Exception> closeFailure = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                try {
                    recovering.close("test", false, false);
                } catch (Exception e) {
                    closeFailure.set(e);
                }
            }, "closer");
            closer.start();
            closer.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("close() must not be held up by a remote store download that is still in flight", closer.isAlive());
            assertThat(closeFailure.get(), nullValue());
            assertThat(recovering.state(), equalTo(IndexShardState.CLOSED));

            // The download was cancelled by the close, not by this test releasing it.
            assertTrue("closing the shard must interrupt the in-flight download", gate.interrupted.await(30, TimeUnit.SECONDS));
            recovery.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse("the recovery must have given up on the closed shard", recovery.isAlive());
            // The hydration reports the shard closed, and StoreRecovery treats that as "got closed on us, just ignore
            // this recovery": no failure, no shard failure, just a recovery that did not happen.
            assertThat("the recovery must not fail the shard, got " + recoveryFailure.get(), recoveryFailure.get(), nullValue());
            assertThat("a recovery of a closed shard is ignored", recovered.get(), equalTo(false));
            assertThat("no engine may be opened on a closed shard", recovering.getIndexerOrNull(), nullValue());
        } finally {
            gate.release.countDown();
            recovery.join(TimeUnit.SECONDS.toMillis(30));
            IOUtils.close(recovering.store());
        }
    }

    /**
     * Recovery completes from the hydrated local files without reading the repository during engine construction.
     */
    public void testRecoveryStillHydratesAndOpensWhenNotClosed() throws Exception {
        String remoteStorePath = createTempDir().toString();
        AtomicBoolean rejectReadsOnEngineCreation = new AtomicBoolean();
        EngineBackedIndexerFactory delegate = new EngineBackedIndexerFactory(new InternalEngineFactory());
        IndexerFactory indexerFactory = config -> {
            if (rejectReadsOnEngineCreation.get()) {
                gate.rejectReads = true;
            }
            return delegate.createIndexer(config);
        };
        IndexShard shard = newStartedShard(
            true,
            Settings.builder()
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, remoteStorePath + "__test")
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, remoteStorePath + "__test")
                .build(),
            indexerFactory
        );
        int numDocs = between(1, 5);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        IOUtils.close(() -> shard.close("test", false, false), shard.store());

        IndexShard recovered = reinitShard(shard);
        rejectReadsOnEngineCreation.set(true);
        recoverShardFromStore(recovered);
        assertDocCount(recovered, numDocs);
        closeShards(recovered);
    }
}
