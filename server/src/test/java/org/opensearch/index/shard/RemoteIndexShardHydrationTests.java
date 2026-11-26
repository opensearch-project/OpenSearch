/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.mockstore.BlobStoreWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * A recovery of a remote-store shard hydrates segments and translog from the remote store before it opens the engine.
 * That hydration must behave like a peer recovery's phase 1: it runs outside the engine open critical section, so that
 * {@link IndexShard#close} - called from under the cluster applier's lock - is never held up by remote I/O, and closing
 * the shard aborts it (issue #15277).
 */
public class RemoteIndexShardHydrationTests extends IndexShardTestCase {

    /** Holds translog downloads when armed. */
    private static final class DownloadGate {
        volatile boolean armed;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch interrupted = new CountDownLatch(1);

        void pass(String blobName) throws IOException {
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
    private final AtomicInteger remoteAccesses = new AtomicInteger();

    @Override
    protected BlobStore wrapRemoteStoreBlobStore(BlobStore blobStore) {
        return new BlobStoreWrapper(blobStore) {
            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return instrument(super.blobContainer(path));
            }
        };
    }

    /**
     * The remote segment store is built straight from a blob container rather than from the repository's blob store,
     * so it has to be instrumented separately for a read of it to be visible here.
     */
    @Override
    protected BlobContainer getBlobContainer(Path f) throws IOException {
        return instrument(super.getBlobContainer(f));
    }

    /** Counts every read of the remote store, and holds translog downloads when the gate is armed. */
    private BlobContainer instrument(BlobContainer delegate) {
        return new FilterBlobContainer(delegate) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return instrument(child);
            }

            @Override
            public Map<String, BlobMetadata> listBlobs() throws IOException {
                remoteAccesses.incrementAndGet();
                return super.listBlobs();
            }

            @Override
            public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
                remoteAccesses.incrementAndGet();
                return super.listBlobsByPrefix(blobNamePrefix);
            }

            @Override
            public InputStream readBlob(String blobName) throws IOException {
                remoteAccesses.incrementAndGet();
                gate.pass(blobName);
                return super.readBlob(blobName);
            }

            @Override
            public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
                remoteAccesses.incrementAndGet();
                gate.pass(blobName);
                return super.readBlobWithMetadata(blobName);
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
     * Recovery completes after the out-of-lock hydration and the constructor's incremental reconciliation.
     */
    public void testRecoveryStillHydratesAndOpensWhenNotClosed() throws Exception {
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
        IOUtils.close(() -> shard.close("test", false, false), shard.store());

        IndexShard recovered = reinitShard(shard);
        recoverShardFromStore(recovered);
        assertDocCount(recovered, numDocs);
        closeShards(recovered);
    }

    /**
     * In segments_only mode the remote store holds segments and never the operation stream, so operations that have
     * been acknowledged but not yet flushed exist only in the local translog. Hydrating such a primary would rewrite
     * its Lucene commit from the infos held remotely and drop the reference to them, and unlike a fully remote backed
     * primary there is no remote translog to bring them back. It must therefore recover from its own translog. That
     * those operations really do survive a restart is covered by
     * {@code SegmentsOnlyRemoteStoreIT#testUnflushedWritesSurviveAFullClusterRestart}, which needs a real cluster
     * because nothing in this harness uploads segments to the remote store.
     */
    public void testSegmentsOnlyPrimaryRecoversFromItsLocalTranslog() throws Exception {
        String remoteStorePath = createTempDir().toString();
        IndexShard shard = newStartedShard(
            true,
            segmentsOnlySettings(remoteStorePath),
            new EngineBackedIndexerFactory(new InternalEngineFactory())
        );
        int numDocs = between(1, 5);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        assertFalse("the translog must stay local in segments_only mode", shard.indexSettings().isTranslogDurableInRemoteStore());
        assertTrue("the segments must still be remote backed", shard.indexSettings().isRemoteStoreEnabled());
        // No flush, so the operations are in the translog and not in any Lucene commit.
        IOUtils.close(() -> shard.close("test", false, false), shard.store());

        IndexShard recovered = reinitShard(shard);
        recoverShardFromStore(recovered);
        assertDocCount(recovered, numDocs);
        closeShards(recovered);
    }

    /**
     * A segments_only replica is fed by node-to-node replication, so the remote store never holds the operations it
     * has acknowledged. Opening it must therefore take the peer recovery path a document replication replica takes
     * and leave the remote store alone, rather than hydrating from it and rewriting the local Lucene commit.
     */
    public void testSegmentsOnlyReplicaIsNotHydratedFromTheRemoteStore() throws Exception {
        String remoteStorePath = createTempDir().toString();
        IndexShard shard = newStartedShard(
            true,
            segmentsOnlySettings(remoteStorePath),
            new EngineBackedIndexerFactory(new InternalEngineFactory())
        );
        int numDocs = between(1, 5);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        shard.updateLocalCheckpointForShard(shard.routingEntry().allocationId().getId(), shard.getLocalCheckpoint());
        shard.sync();
        final long globalCheckpoint = shard.getLastSyncedGlobalCheckpoint();
        assertThat(globalCheckpoint, equalTo((long) numDocs - 1));
        IOUtils.close(() -> shard.close("test", false, false), shard.store());

        // A segment replication replica runs an NRT engine, which is what decides that a replayed operation goes to
        // the translog and not into Lucene, so the shard has to be reopened with one for the replay below to behave
        // the way it does on a real replica.
        IndexShard replica = reinitShard(
            shard,
            TestShardRouting.newShardRouting(
                shard.shardId(),
                shard.routingEntry().currentNodeId(),
                false,
                ShardRoutingState.INITIALIZING,
                RecoverySource.PeerRecoverySource.INSTANCE
            ),
            shard.indexSettings().getIndexMetadata(),
            new EngineBackedIndexerFactory(new NRTReplicationEngineFactory()),
            shard.engineConfigFactory,
            null
        );
        assertFalse("the translog must stay local in segments_only mode", replica.indexSettings().isTranslogDurableInRemoteStore());
        assertTrue("the segments must still be remote backed", replica.indexSettings().isRemoteStoreEnabled());

        DiscoveryNode node = IndexShardTestUtils.getFakeSegmentsOnlyNode(replica.routingEntry().currentNodeId());
        replica.markAsRecovering("peer recovery", new RecoveryState(replica.routingEntry(), node, node));
        replica.prepareForIndexRecovery();
        remoteAccesses.set(0);
        // This is the step of a peer recovery that opens the engine on whatever the replica already has on disk. An
        // NRT engine has no IndexWriter to replay a translog into, so the operations below the global checkpoint are
        // left to the peer, which is what the unassigned starting sequence number says.
        assertThat(replica.recoverLocallyAndFetchStartSeqNo(true), equalTo(UNASSIGNED_SEQ_NO));
        assertThat("a segments_only replica must not touch the remote store while opening its engine", remoteAccesses.get(), equalTo(0));
        closeShards(replica);
    }

    /** A remote segment repository and deliberately no translog repository, which is what segments_only mode is. */
    private static Settings segmentsOnlySettings(String remoteStorePath) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, remoteStorePath + "__test")
            .build();
    }
}
