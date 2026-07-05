/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.listener.TranslogEventListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.opensearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.equalTo;

public class InternalTranslogManagerTests extends TranslogManagerTestCase {

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
}
