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
                () -> Boolean.TRUE
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
                () -> Boolean.TRUE
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
                () -> Boolean.TRUE
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
                () -> Boolean.TRUE
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
                () -> Boolean.TRUE
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
                () -> Boolean.TRUE
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
                () -> Boolean.TRUE
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
}
