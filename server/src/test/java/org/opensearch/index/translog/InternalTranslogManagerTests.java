/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.opensearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalTranslogManagerTests extends TranslogManagerTestCase {

    private static final IndexSettings REMOTE_TRANSLOG_INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "seg-repo")
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "txlog-repo")
            .build()
    );

    private static final IndexSettings DOC_REP_INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    public void testRecoveryFromTranslog() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicBoolean beginTranslogRecoveryInvoked = new AtomicBoolean(false);
        final AtomicBoolean onTranslogRecoveryInvoked = new AtomicBoolean(false);
        InternalTranslogManager translogManager = null;

        LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        try {
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                createTranslogDeletionPolicy(INDEX_SETTINGS),
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> tracker,
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );
            final int docs = randomIntBetween(1, 100);
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                Engine.Index index = indexForDoc(doc);
                Engine.IndexResult indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), i, true);
                tracker.markSeqNoAsProcessed(i);
                translogManager.add(new Translog.Index(index, indexResult));
                translogManager.rollTranslogGeneration();
            }
            long maxSeqNo = tracker.getMaxSeqNo();
            assertEquals(maxSeqNo + 1, translogManager.getTranslogStats().getUncommittedOperations());
            assertEquals(maxSeqNo + 1, translogManager.getTranslogStats().estimatedNumberOfOperations());

            translogManager.syncTranslog();
            translogManager.close();
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                createTranslogDeletionPolicy(INDEX_SETTINGS),
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED),
                translogUUID,
                new TranslogEventListener() {
                    @Override
                    public void onAfterTranslogRecovery() {
                        onTranslogRecoveryInvoked.set(true);
                    }

                    @Override
                    public void onBeginTranslogRecovery() {
                        beginTranslogRecoveryInvoked.set(true);
                    }
                },
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );
            AtomicInteger opsRecovered = new AtomicInteger();
            int opsRecoveredFromTranslog = translogManager.recoverFromTranslog((snapshot) -> {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    opsRecovered.incrementAndGet();
                }
                return opsRecovered.get();
            }, NO_OPS_PERFORMED, Long.MAX_VALUE);

            assertEquals(maxSeqNo + 1, opsRecovered.get());
            assertEquals(maxSeqNo + 1, opsRecoveredFromTranslog);

            assertTrue(beginTranslogRecoveryInvoked.get());
            assertTrue(onTranslogRecoveryInvoked.get());

        } finally {
            translogManager.close();
        }
    }

    public void testTranslogRollsGeneration() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        InternalTranslogManager translogManager = null;
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        try {
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                createTranslogDeletionPolicy(INDEX_SETTINGS),
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> tracker,
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );
            final int docs = randomIntBetween(1, 100);
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                Engine.Index index = indexForDoc(doc);
                Engine.IndexResult indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), i, true);
                tracker.markSeqNoAsProcessed(i);
                translogManager.add(new Translog.Index(index, indexResult));
                translogManager.rollTranslogGeneration();
            }
            long maxSeqNo = tracker.getMaxSeqNo();
            assertEquals(maxSeqNo + 1, translogManager.getTranslogStats().getUncommittedOperations());
            assertEquals(maxSeqNo + 1, translogManager.getTranslogStats().estimatedNumberOfOperations());

            translogManager.syncTranslog();
            translogManager.close();
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                createTranslogDeletionPolicy(INDEX_SETTINGS),
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED),
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );
            AtomicInteger opsRecovered = new AtomicInteger();
            int opsRecoveredFromTranslog = translogManager.recoverFromTranslog((snapshot) -> {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    opsRecovered.incrementAndGet();
                }
                return opsRecovered.get();
            }, NO_OPS_PERFORMED, Long.MAX_VALUE);

            assertEquals(maxSeqNo + 1, opsRecovered.get());
            assertEquals(maxSeqNo + 1, opsRecoveredFromTranslog);
        } finally {
            translogManager.close();
        }
    }

    public void testTrimOperationsFromTranslog() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        InternalTranslogManager translogManager = null;
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        try {
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                createTranslogDeletionPolicy(INDEX_SETTINGS),
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> tracker,
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );
            final int docs = randomIntBetween(1, 100);
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                Engine.Index index = indexForDoc(doc);
                Engine.IndexResult indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), i, true);
                tracker.markSeqNoAsProcessed(i);
                translogManager.add(new Translog.Index(index, indexResult));
            }
            long maxSeqNo = tracker.getMaxSeqNo();
            assertEquals(maxSeqNo + 1, translogManager.getTranslogStats().getUncommittedOperations());
            assertEquals(maxSeqNo + 1, translogManager.getTranslogStats().estimatedNumberOfOperations());

            primaryTerm.set(randomLongBetween(primaryTerm.get(), Long.MAX_VALUE));
            translogManager.rollTranslogGeneration();
            translogManager.trimOperationsFromTranslog(primaryTerm.get(), NO_OPS_PERFORMED); // trim everything in translog

            translogManager.close();
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                createTranslogDeletionPolicy(INDEX_SETTINGS),
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED),
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );
            AtomicInteger opsRecovered = new AtomicInteger();
            int opsRecoveredFromTranslog = translogManager.recoverFromTranslog((snapshot) -> {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    opsRecovered.incrementAndGet();
                }
                return opsRecovered.get();
            }, NO_OPS_PERFORMED, Long.MAX_VALUE);

            assertEquals(0, opsRecovered.get());
            assertEquals(0, opsRecoveredFromTranslog);
        } finally {
            translogManager.close();
        }
    }

    public void testTranslogSync() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        AtomicBoolean syncListenerInvoked = new AtomicBoolean();
        InternalTranslogManager translogManager = null;
        final AtomicInteger maxSeqNo = new AtomicInteger(randomIntBetween(0, 128));
        final AtomicInteger localCheckpoint = new AtomicInteger(randomIntBetween(0, maxSeqNo.get()));
        try {
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            AtomicReference<InternalTranslogManager> translogManagerAtomicReference = new AtomicReference<>();
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                createTranslogDeletionPolicy(INDEX_SETTINGS),
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> new LocalCheckpointTracker(maxSeqNo.get(), localCheckpoint.get()),
                translogUUID,
                new TranslogEventListener() {
                    @Override
                    public void onAfterTranslogSync() {
                        try {
                            translogManagerAtomicReference.get().trimUnreferencedReaders();
                            syncListenerInvoked.set(true);
                        } catch (IOException ex) {
                            fail("Failed due to " + ex);
                        }
                    }
                },
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );
            translogManagerAtomicReference.set(translogManager);
            Engine.Index index = indexForDoc(doc);
            Engine.IndexResult indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), 1, false);
            translogManager.add(new Translog.Index(index, indexResult));

            translogManager.syncTranslog();

            assertThat(translogManager.getTranslog().currentFileGeneration(), equalTo(2L));
            assertThat(translogManager.getTranslog().getMinFileGeneration(), equalTo(2L));
            assertTrue(syncListenerInvoked.get());
        } finally {
            translogManager.close();
        }
    }

    /**
     * {@link InternalTranslogManager#acquireHistoryRetentionLock()} must return a lock backed by
     * {@link Translog#acquireRetentionLock()} that pins a translog generation in the deletion policy while
     * it is held, and releases that generation when closed. This is the translog-layer support that
     * {@link org.opensearch.index.engine.DataFormatAwareEngine} relies on to keep history available for
     * the duration of peer recovery / primary relocation.
     */
    public void testAcquireHistoryRetentionLock() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        // Hold a reference to the deletion policy so we can observe the retention locks it tracks.
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(INDEX_SETTINGS);
        InternalTranslogManager translogManager = null;
        try {
            translogManager = new InternalTranslogManager(
                new TranslogConfig(shardId, primaryTranslogDir, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE, "", false),
                primaryTerm,
                globalCheckpoint::get,
                deletionPolicy,
                shardId,
                new ReleasableLock(new ReentrantReadWriteLock().readLock()),
                () -> tracker,
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                () -> {},
                new InternalTranslogFactory(),
                () -> Boolean.TRUE,
                TranslogOperationHelper.DEFAULT
            );

            // Index a few operations spread over multiple generations.
            final int docs = randomIntBetween(1, 10);
            for (int i = 0; i < docs; i++) {
                final ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocumentWithTextField(), SOURCE, null);
                final Engine.Index index = indexForDoc(doc);
                final Engine.IndexResult indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), i, true);
                tracker.markSeqNoAsProcessed(i);
                translogManager.add(new Translog.Index(index, indexResult));
                translogManager.rollTranslogGeneration();
            }

            // The manager must expose the same deletion policy instance the lock pins against.
            assertSame(deletionPolicy, translogManager.getTranslog().getDeletionPolicy());

            // No retention locks are held before acquiring one.
            assertEquals(0, deletionPolicy.pendingTranslogRefCount());
            deletionPolicy.assertNoOpenTranslogRefs();

            // Acquiring the history retention lock pins a translog generation.
            final Closeable retentionLock = translogManager.acquireHistoryRetentionLock();
            assertNotNull(retentionLock);
            assertEquals(1, deletionPolicy.pendingTranslogRefCount());
            // While the lock is held there is an open translog reference.
            expectThrows(AssertionError.class, deletionPolicy::assertNoOpenTranslogRefs);

            // Releasing the lock releases the pinned generation.
            retentionLock.close();
            assertEquals(0, deletionPolicy.pendingTranslogRefCount());
            deletionPolicy.assertNoOpenTranslogRefs();
        } finally {
            translogManager.close();
        }
    }

    public void testRemoteTranslogBytesControlPeriodicFlush() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(true);
        when(remoteTranslog.add(any())).thenReturn(new Translog.Location(1, 0, 100));

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true)) {
            translogManager.add(mock(Translog.Operation.class));

            assertFalse(translogManager.shouldPeriodicallyFlush(0, 101));
            assertTrue(translogManager.shouldPeriodicallyFlush(0, 100));

            when(remoteTranslog.isBytesTrackingSettingEnabled()).thenReturn(false);
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 100));
        }
    }

    public void testRemoteTranslogBytesResetOnlyAfterSuccessfulCommit() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(true);
        when(remoteTranslog.add(any())).thenReturn(
            new Translog.Location(1, 0, 100),
            new Translog.Location(1, 100, 20),
            new Translog.Location(1, 120, 30)
        );

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true)) {
            Translog.Operation operation = mock(Translog.Operation.class);
            translogManager.add(operation);
            translogManager.startIndexCommit();
            translogManager.add(operation);
            translogManager.finishIndexCommit(true);

            assertFalse(translogManager.shouldPeriodicallyFlush(0, 21));
            assertTrue(translogManager.shouldPeriodicallyFlush(0, 20));

            translogManager.add(operation);
            translogManager.startIndexCommit();
            translogManager.finishIndexCommit(false);

            assertTrue(translogManager.shouldPeriodicallyFlush(0, 50));
        }
    }

    public void testMaxRemoteTranslogReadersRemainsIndependentFlushTrigger() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(false);
        when(remoteTranslog.shouldFlush()).thenReturn(true);

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog)) {
            assertTrue(translogManager.shouldPeriodicallyFlush(0, Long.MAX_VALUE));
        }
    }

    public void testRemoteTranslogBytesAreNotTrackedWhenDisabled() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(false);
        when(remoteTranslog.add(any())).thenReturn(new Translog.Location(1, 0, 100), new Translog.Location(1, 100, 20));

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true)) {
            Translog.Operation operation = mock(Translog.Operation.class);
            translogManager.add(operation);

            when(remoteTranslog.isBytesTrackingSettingEnabled()).thenReturn(true);
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 100));

            translogManager.add(operation);
            assertTrue(translogManager.shouldPeriodicallyFlush(0, 20));
        }
    }

    public void testRemoteTranslogBytesAreNotTrackedForUnsupportedEngine() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(true);
        when(remoteTranslog.add(any())).thenReturn(new Translog.Location(1, 0, 100));

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog)) {
            translogManager.add(mock(Translog.Operation.class));

            assertFalse(translogManager.isTranslogBytesTrackingEnabled());
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 100));
        }
    }

    /**
     * A node with a translog repository configured hands a {@link RemoteFsTranslog} to the primary of a plain document
     * replication index too. Those shards must keep the untouched size computation.
     */
    public void testRemoteTranslogBytesAreNotTrackedForDocumentReplicationIndex() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(true);
        when(remoteTranslog.add(any())).thenReturn(new Translog.Location(1, 0, 100));

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true, DOC_REP_INDEX_SETTINGS)) {
            translogManager.add(mock(Translog.Operation.class));

            assertFalse(translogManager.isTranslogBytesTrackingEnabled());
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 100));
        }
    }

    public void testTranslogBytesTrackerIsSeededWithUncommittedBytes() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(true);
        // The shard recovered 500 uncommitted bytes before this manager, and therefore this tracker, existed.
        when(remoteTranslog.sizeInBytesByMinGen(anyLong())).thenReturn(500L);

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true)) {
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 501));
            assertTrue(translogManager.shouldPeriodicallyFlush(0, 500));
        }
    }

    public void testTranslogBytesTrackerSeedIsReleasedByFirstCommit() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(true);
        when(remoteTranslog.sizeInBytesByMinGen(anyLong())).thenReturn(500L);
        when(remoteTranslog.add(any())).thenReturn(new Translog.Location(1, 500, 20));

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true)) {
            translogManager.startIndexCommit();
            translogManager.finishIndexCommit(true);
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 1));

            // The seed is a one time correction, so the next threshold has to be reached by new operations alone even
            // though the translog files themselves have not shrunk.
            translogManager.add(mock(Translog.Operation.class));
            assertTrue(translogManager.shouldPeriodicallyFlush(0, 20));
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 21));
        }
    }

    public void testTranslogBytesTrackerIsNotSeededWhenTrackingIsDisabled() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(false);
        when(remoteTranslog.sizeInBytesByMinGen(anyLong())).thenReturn(500L);

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true)) {
            // While disabled the legacy computation runs, which reports the same 500 bytes but keeps its own guards.
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 501));

            // Enabling the setting now seeds from the translog, so the threshold is met without any new operation.
            when(remoteTranslog.isBytesTrackingSettingEnabled()).thenReturn(true);
            assertTrue(translogManager.shouldPeriodicallyFlush(0, 500));
        }
    }

    /**
     * Documents an accepted approximation. Commits that run while the setting is off release nothing, so re-enabling it
     * resumes from a stale count. The error is always toward flushing earlier than needed and the first commit after
     * re-enabling repairs it.
     */
    public void testTrackedBytesRemainStaleAcrossDisableEnableCycle() throws IOException {
        RemoteFsTranslog remoteTranslog = mockRemoteTranslog(true);
        when(remoteTranslog.add(any())).thenReturn(new Translog.Location(1, 0, 100), new Translog.Location(1, 100, 30));

        try (InternalTranslogManager translogManager = createTranslogManager(remoteTranslog, true)) {
            Translog.Operation operation = mock(Translog.Operation.class);
            translogManager.add(operation);

            // Disabled: the legacy computation decides and further operations are not counted.
            when(remoteTranslog.isBytesTrackingSettingEnabled()).thenReturn(false);
            translogManager.add(operation);
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 100));

            // Re-enabled: the 100 bytes counted before the cycle are still there, so the threshold arrives early.
            when(remoteTranslog.isBytesTrackingSettingEnabled()).thenReturn(true);
            assertTrue(translogManager.shouldPeriodicallyFlush(0, 100));

            // A single commit brings the count back in line.
            translogManager.startIndexCommit();
            translogManager.finishIndexCommit(true);
            assertFalse(translogManager.shouldPeriodicallyFlush(0, 1));
        }
    }

    private RemoteFsTranslog mockRemoteTranslog(boolean bytesTrackingSettingEnabled) {
        RemoteFsTranslog remoteTranslog = mock(RemoteFsTranslog.class);
        Translog.TranslogGeneration generation = new Translog.TranslogGeneration(translogUUID, 1);
        when(remoteTranslog.getGeneration()).thenReturn(generation);
        when(remoteTranslog.isBytesTrackingSettingEnabled()).thenReturn(bytesTrackingSettingEnabled);
        when(remoteTranslog.getMinUnreferencedSeqNoInSegments(anyLong())).thenReturn(0L);
        when(remoteTranslog.getMinGenerationForSeqNo(anyLong())).thenReturn(generation);
        when(remoteTranslog.sizeInBytesByMinGen(anyLong())).thenReturn(0L);
        when(remoteTranslog.getDeletionPolicy()).thenReturn(createTranslogDeletionPolicy(INDEX_SETTINGS));
        return remoteTranslog;
    }

    private InternalTranslogManager createTranslogManager(Translog translog) throws IOException {
        return createTranslogManager(translog, false);
    }

    private InternalTranslogManager createTranslogManager(Translog translog, boolean releasesTrackedBytesOnCommit) throws IOException {
        return createTranslogManager(translog, releasesTrackedBytesOnCommit, REMOTE_TRANSLOG_INDEX_SETTINGS);
    }

    private InternalTranslogManager createTranslogManager(
        Translog translog,
        boolean releasesTrackedBytesOnCommit,
        IndexSettings indexSettings
    ) throws IOException {
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        return new InternalTranslogManager(
            new TranslogConfig(shardId, primaryTranslogDir, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false),
            primaryTerm,
            () -> NO_OPS_PERFORMED,
            createTranslogDeletionPolicy(indexSettings),
            shardId,
            new ReleasableLock(new ReentrantReadWriteLock().readLock()),
            () -> tracker,
            translogUUID,
            TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
            () -> {},
            new StubTranslogFactory(translog),
            () -> true,
            TranslogOperationHelper.DEFAULT,
            releasesTrackedBytesOnCommit
        );
    }

    private static class StubTranslogFactory implements TranslogFactory {
        private final Translog translog;

        StubTranslogFactory(Translog translog) {
            this.translog = translog;
        }

        @Override
        public Translog newTranslog(
            TranslogConfig config,
            String translogUUID,
            TranslogDeletionPolicy deletionPolicy,
            LongSupplier globalCheckpointSupplier,
            LongSupplier primaryTermSupplier,
            LongConsumer persistedSequenceNumberConsumer,
            BooleanSupplier startedPrimarySupplier
        ) {
            return translog;
        }

        @Override
        public Translog newTranslog(
            TranslogConfig config,
            String translogUUID,
            TranslogDeletionPolicy deletionPolicy,
            LongSupplier globalCheckpointSupplier,
            LongSupplier primaryTermSupplier,
            LongConsumer persistedSequenceNumberConsumer,
            BooleanSupplier startedPrimarySupplier,
            TranslogOperationHelper translogOperationHelper
        ) {
            return translog;
        }
    }
}
