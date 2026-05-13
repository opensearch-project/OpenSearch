/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CommitFileManager;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for {@link IndexFileDeleter}.
 */
public class IndexFileDeleterTests extends OpenSearchTestCase {

    private static Map<String, String> commitUserData(long maxSeqNo, long localCP, String translogUUID) {
        Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCP));
        userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
        return userData;
    }

    private static Segment segment(long gen, String format, String... files) {
        WriterFileSet wfs = new WriterFileSet("/data", gen, Set.of(files), files.length, 0L);
        return new Segment(gen, Map.of(format, wfs));
    }

    private static CatalogSnapshot snapshot(long gen, List<Segment> segments, Map<String, String> userData) {
        return new DataformatAwareCatalogSnapshot(gen, gen, 0L, segments, 0L, userData);
    }

    static class TrackingFileDeleter implements FileDeleter {
        final Set<String> deletedFiles = new HashSet<>();

        @Override
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) {
            for (Collection<String> files : filesToDelete.values()) {
                deletedFiles.addAll(files);
            }
            return Map.of();
        }

        boolean hasDeletedFile(String file) {
            return deletedFiles.contains(file);
        }
    }

    // ---- addFileReferences / removeFileReferences ----

    public void testAddFileReferencesTracksNewFiles() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet", "b.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            tracker,
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        Map<String, Collection<String>> newFiles = deleter.addFileReferences(
            snapshot(2, List.of(segment(0, "parquet", "a.parquet", "c.parquet")), commitUserData(200, 200, "uuid"))
        );

        // a.parquet already existed (refCount 1→2), c.parquet is new (0→1)
        assertTrue(newFiles.get("parquet").contains("c.parquet"));
        assertFalse(newFiles.get("parquet").contains("a.parquet"));
    }

    public void testRemoveFileReferencesDeletesOrphanedFiles() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet", "b.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            tracker,
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        // Add cs2 sharing a.parquet but not b.parquet
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "a.parquet", "c.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);

        // Remove cs1's refs
        deleter.removeFileReferences(cs1);

        // b.parquet was only in cs1 → deleted
        assertTrue(tracker.hasDeletedFile("b.parquet"));
        // a.parquet shared with cs2 → NOT deleted
        assertFalse(tracker.hasDeletedFile("a.parquet"));
    }

    // ---- onCommit ----

    public void testOnCommitDeletesOldCommitFiles() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        CatalogSnapshot cs1 = snapshot(
            1,
            List.of(segment(0, "parquet", "old_a.parquet"), segment(1, "parquet", "old_b.parquet")),
            commitUserData(100, 100, "uuid")
        );

        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, Map.of(), List.of(cs1), null, null);

        // Refresh: cs2 with merged files
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(2, "parquet", "new_merged.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);

        // Simulate manager releasing cs1 (as commitNewSnapshot would)
        cs1.decRef();

        // Commit cs2, advance globalCP
        globalCP.set(200);
        deleter.onCommit(cs2);

        // cs1 deleted by policy → old_a and old_b should be deleted
        assertTrue(tracker.hasDeletedFile("old_a.parquet"));
        assertTrue(tracker.hasDeletedFile("old_b.parquet"));
        assertFalse(tracker.hasDeletedFile("new_merged.parquet"));
    }

    public void testOnCommitPreservesSharedFiles() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        CatalogSnapshot cs1 = snapshot(
            1,
            List.of(segment(0, "parquet", "shared.parquet"), segment(1, "parquet", "only_cs1.parquet")),
            commitUserData(100, 100, "uuid")
        );

        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, Map.of(), List.of(cs1), null, null);

        // cs2 keeps shared.parquet, adds new file
        CatalogSnapshot cs2 = snapshot(
            2,
            List.of(segment(0, "parquet", "shared.parquet"), segment(2, "parquet", "only_cs2.parquet")),
            commitUserData(200, 200, "uuid")
        );
        deleter.addFileReferences(cs2);

        // Simulate manager releasing cs1
        cs1.decRef();

        globalCP.set(200);
        deleter.onCommit(cs2);

        // only_cs1 deleted, shared survives
        assertTrue(tracker.hasDeletedFile("only_cs1.parquet"));
        assertFalse(tracker.hasDeletedFile("shared.parquet"));
    }

    // ---- revisitPolicy ----

    public void testRevisitPolicyDeletesPreviouslyProtectedCommit() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "cs1_file.parquet")), commitUserData(100, 100, "uuid"));
        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, Map.of(), List.of(cs1), null, null);

        // Hold cs1 via snapshot protection
        var held = policy.acquireCommittedSnapshot(false);

        // Add and commit cs2
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "cs2_file.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);

        // Simulate manager releasing cs1
        cs1.decRef();

        globalCP.set(200);
        deleter.onCommit(cs2);

        // cs1 protected — not deleted
        assertFalse(tracker.hasDeletedFile("cs1_file.parquet"));

        // Release hold
        held.close();

        // revisitPolicy should now delete cs1
        deleter.revisitPolicy();
        assertTrue(tracker.hasDeletedFile("cs1_file.parquet"));
    }

    // ---- Multi-format ----

    public void testMultiFormatFileDeletion() throws IOException {
        TrackingFileDeleter parquetTracker = new TrackingFileDeleter();
        TrackingFileDeleter luceneTracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        // cs1 has files in two formats
        Segment seg = new Segment(
            0,
            Map.of(
                "parquet",
                new WriterFileSet("/data", 0, Set.of("data.parquet"), 1, 0L),
                "lucene",
                new WriterFileSet("/data", 0, Set.of("_0.cfs"), 1, 0L)
            )
        );
        CatalogSnapshot cs1 = snapshot(1, List.of(seg), commitUserData(100, 100, "uuid"));
        IndexFileDeleter deleter = new IndexFileDeleter(policy, files -> {
            Map<String, Collection<String>> failed = new java.util.HashMap<>();
            for (Map.Entry<String, Collection<String>> e : files.entrySet()) {
                if ("parquet".equals(e.getKey())) {
                    failed.putAll(parquetTracker.deleteFiles(Map.of(e.getKey(), e.getValue())));
                } else if ("lucene".equals(e.getKey())) {
                    failed.putAll(luceneTracker.deleteFiles(Map.of(e.getKey(), e.getValue())));
                }
            }
            return failed;
        }, Map.of(), List.of(cs1), null, null);

        // cs2 has completely different files
        Segment seg2 = new Segment(
            1,
            Map.of(
                "parquet",
                new WriterFileSet("/data", 1, Set.of("data2.parquet"), 1, 0L),
                "lucene",
                new WriterFileSet("/data", 1, Set.of("_1.cfs"), 1, 0L)
            )
        );
        CatalogSnapshot cs2 = snapshot(2, List.of(seg2), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);

        // Simulate manager releasing cs1
        cs1.decRef();

        globalCP.set(200);
        deleter.onCommit(cs2);

        // Both formats' old files should be deleted
        assertTrue(parquetTracker.hasDeletedFile("data.parquet"));
        assertTrue(luceneTracker.hasDeletedFile("_0.cfs"));
        assertFalse(parquetTracker.hasDeletedFile("data2.parquet"));
        assertFalse(luceneTracker.hasDeletedFile("_1.cfs"));
    }

    // ---- Lifecycle: commit ref keeps snapshot alive ----

    public void testCommitRefKeepsSnapshotAlive() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "cs1.parquet")), commitUserData(100, 100, "uuid"));
        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, Map.of(), List.of(cs1), null, null);

        // cs1 has refCount=2 (manager + commit from constructor)
        assertEquals(2, cs1.refCount());

        // Simulate manager releasing its ref (like commitNewSnapshot replacing it)
        cs1.decRef(); // manager releases → refCount=1 (commit still holds)
        assertEquals(1, cs1.refCount());

        // Files NOT deleted — commit ref keeps it alive
        assertTrue(tracker.deletedFiles.isEmpty());

        // Add cs2 and commit
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "cs2.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);
        globalCP.set(200);
        deleter.onCommit(cs2);

        // Policy deletes cs1 → decRef → refCount=0 → removeFileReferences
        assertEquals(0, cs1.refCount());
        assertTrue(tracker.hasDeletedFile("cs1.parquet"));
    }

    // ---- deleteOrphanedFiles ----

    public void testDeleteOrphanedFilesOnInit() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();

        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        Path shardDir = tempDir.resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        Files.createDirectories(shardDir);

        Path parquetDir = shardDir.resolve("parquet");
        Files.createDirectories(parquetDir);
        Files.createFile(parquetDir.resolve("known.parquet"));
        Files.createFile(parquetDir.resolve("orphan1.parquet"));
        Files.createFile(parquetDir.resolve("orphan2.parquet"));

        ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "known.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            tracker,
            Map.of(),
            List.of(cs1),
            shardPath,
            null
        );

        assertTrue(tracker.hasDeletedFile("orphan1.parquet"));
        assertTrue(tracker.hasDeletedFile("orphan2.parquet"));
        assertFalse(tracker.hasDeletedFile("known.parquet"));
    }

    public void testDeleteOrphanedFilesSkipsMissingDirectory() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        Path shardDir = tempDir.resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        Files.createDirectories(shardDir);

        ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            tracker,
            Map.of(),
            List.of(cs1),
            shardPath,
            null
        );
        assertTrue(tracker.deletedFiles.isEmpty());
    }

    // ---- Fix #3: Partial deletion failure tracking via pendingDeletes ----

    /**
     * A FileDeleter that fails the first N calls, then succeeds on subsequent calls.
     * Simulates transient I/O failure (e.g., remote store timeout).
     */
    static class FailNTimesThenSucceedDeleter implements FileDeleter {
        final Set<String> deletedFiles = new HashSet<>();
        private int failuresRemaining;

        FailNTimesThenSucceedDeleter(int failCount) {
            this.failuresRemaining = failCount;
        }

        @Override
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
            if (failuresRemaining > 0) {
                failuresRemaining--;
                throw new IOException("Simulated transient I/O failure");
            }
            for (Collection<String> files : filesToDelete.values()) {
                deletedFiles.addAll(files);
            }
            return Map.of();
        }
    }

    /**
     * A FileDeleter that always throws IOException. Simulates persistent I/O failure.
     */
    static class AlwaysFailingDeleter implements FileDeleter {
        int deleteAttempts = 0;

        @Override
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
            deleteAttempts++;
            throw new IOException("Persistent I/O failure");
        }
    }

    public void testPartialDeleteFailureTracksPendingDeletes() throws IOException {
        AlwaysFailingDeleter failingDeleter = new AlwaysFailingDeleter();
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet", "b.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            failingDeleter,
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        // Add cs2 with different files, then remove cs1
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "c.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);
        deleter.removeFileReferences(cs1);

        // Files should be in pending deletes since the deleter always fails
        assertTrue("Should have pending deletes after failed deletion", deleter.hasPendingDeletes());
        Map<String, Set<String>> pending = deleter.getPendingDeletes();
        assertTrue("a.parquet should be pending", pending.get("parquet").contains("a.parquet"));
        assertTrue("b.parquet should be pending", pending.get("parquet").contains("b.parquet"));
    }

    public void testPendingDeletesRetriedOnNextRemoveFileReferences() throws IOException {
        // Fail twice: once during executeDeletesWithRetry, once during the automatic
        // retryPendingDeletes inside removeFileReferences. Third call succeeds.
        FailNTimesThenSucceedDeleter deleter1 = new FailNTimesThenSucceedDeleter(2);
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            deleter1,
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        // cs2 and cs3 with different files
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "b.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);
        CatalogSnapshot cs3 = snapshot(3, List.of(segment(2, "parquet", "c.parquet")), commitUserData(300, 300, "uuid"));
        deleter.addFileReferences(cs3);

        // First removal: executeDeletesWithRetry fails (call 1), retryPendingDeletes fails (call 2)
        deleter.removeFileReferences(cs1);
        assertTrue("Should have pending deletes after both attempts failed", deleter.hasPendingDeletes());

        // Second removal: deleter now succeeds (call 3+), retrying a.parquet and deleting b.parquet
        deleter.removeFileReferences(cs2);
        assertTrue("a.parquet should be deleted on retry", deleter1.deletedFiles.contains("a.parquet"));
        assertTrue("b.parquet should be deleted", deleter1.deletedFiles.contains("b.parquet"));
        assertFalse("No more pending deletes", deleter.hasPendingDeletes());
    }

    public void testReReferencingPendingDeleteFileThrowsAssertionError() throws IOException {
        AlwaysFailingDeleter failingDeleter = new AlwaysFailingDeleter();
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "shared.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            failingDeleter,
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        // cs2 does NOT contain shared.parquet
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "other.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);
        deleter.removeFileReferences(cs1);

        // shared.parquet should be pending (deletion failed)
        assertTrue(deleter.hasPendingDeletes());
        assertTrue(deleter.getPendingDeletes().get("parquet").contains("shared.parquet"));

        // Re-introducing a dead file should trigger an assertion error — this should never
        // happen in production because once a segment file's ref count reaches 0, no new
        // snapshot should reference it.
        CatalogSnapshot cs3 = snapshot(3, List.of(segment(2, "parquet", "shared.parquet")), commitUserData(300, 300, "uuid"));
        expectThrows(AssertionError.class, () -> deleter.addFileReferences(cs3));
    }

    public void testRetryPendingDeletesExplicitCall() throws IOException {
        // Fail twice: once during executeDeletesWithRetry, once during the automatic
        // retryPendingDeletes inside removeFileReferences. Third call (explicit retry) succeeds.
        FailNTimesThenSucceedDeleter failTwice = new FailNTimesThenSucceedDeleter(2);
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            failTwice,
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "b.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);
        deleter.removeFileReferences(cs1);

        // Both internal attempts failed, file is still pending
        assertTrue("Should have pending deletes", deleter.hasPendingDeletes());

        // Explicit retry — deleter now succeeds (third call)
        deleter.retryPendingDeletes();
        assertTrue("a.parquet should be deleted on explicit retry", failTwice.deletedFiles.contains("a.parquet"));
        assertFalse("Pending deletes should be empty after successful retry", deleter.hasPendingDeletes());
    }

    public void testPersistentFailureKeepsFilesPending() throws IOException {
        AlwaysFailingDeleter alwaysFails = new AlwaysFailingDeleter();
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "doomed.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            alwaysFails,
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "ok.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);
        deleter.removeFileReferences(cs1);

        assertTrue(deleter.hasPendingDeletes());
        int attemptsAfterRemove = alwaysFails.deleteAttempts;

        // Retry — still fails
        deleter.retryPendingDeletes();
        assertTrue("File should remain pending after persistent failure", deleter.hasPendingDeletes());
        assertTrue("doomed.parquet still pending", deleter.getPendingDeletes().get("parquet").contains("doomed.parquet"));
        assertTrue("Should have attempted more deletes", alwaysFails.deleteAttempts > attemptsAfterRemove);
    }

    // ---- Fix #1: I/O outside the synchronized block ----

    /**
     * A FileDeleter that records which thread performed the deletion and whether
     * the IndexFileDeleter monitor was held at that time.
     */
    static class LockProbeDeleter implements FileDeleter {
        final Set<String> deletedFiles = new HashSet<>();
        final List<Boolean> lockHeldDuringDelete = new ArrayList<>();
        private final Object monitorToProbe;

        LockProbeDeleter(Object monitorToProbe) {
            this.monitorToProbe = monitorToProbe;
        }

        @Override
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) {
            lockHeldDuringDelete.add(Thread.holdsLock(monitorToProbe));
            for (Collection<String> files : filesToDelete.values()) {
                deletedFiles.addAll(files);
            }
            return Map.of();
        }
    }

    public void testDeleteFilesExecutedOutsideSynchronizedBlock() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "old.parquet")), commitUserData(100, 100, "uuid"));
        // Create deleter first, then set up the probe
        IndexFileDeleter deleter = new IndexFileDeleter(policy, new TrackingFileDeleter(), Map.of(), List.of(cs1), null, null);

        // Now create a new deleter with the lock probe, using the deleter instance as the monitor
        LockProbeDeleter probe = new LockProbeDeleter(deleter);

        // We need a fresh deleter with the probe. Rebuild.
        CatalogSnapshot cs1b = snapshot(1, List.of(segment(0, "parquet", "old.parquet")), commitUserData(100, 100, "uuid"));
        IndexFileDeleter deleterWithProbe = new IndexFileDeleter(policy, probe, Map.of(), List.of(cs1b), null, null);

        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "new.parquet")), commitUserData(200, 200, "uuid"));
        deleterWithProbe.addFileReferences(cs2);
        cs1b.decRef();
        globalCP.set(200);
        deleterWithProbe.onCommit(cs2);

        // The probe should have recorded that the lock was NOT held during deleteFiles
        assertTrue("old.parquet should have been deleted", probe.deletedFiles.contains("old.parquet"));
        assertFalse("Lock should not be held during file deletion I/O", probe.lockHeldDuringDelete.isEmpty());
        for (Boolean held : probe.lockHeldDuringDelete) {
            assertFalse("IndexFileDeleter monitor should NOT be held during deleteFiles call", held);
        }
    }

    public void testRemoveFileReferencesDoesNotHoldLockDuringIO() throws IOException {
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet")), commitUserData(100, 100, "uuid"));

        // Placeholder deleter for construction
        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            new TrackingFileDeleter(),
            Map.of(),
            List.of(cs1),
            null,
            null
        );

        // Rebuild with lock probe
        LockProbeDeleter probe = new LockProbeDeleter(deleter);
        CatalogSnapshot cs1b = snapshot(1, List.of(segment(0, "parquet", "a.parquet")), commitUserData(100, 100, "uuid"));
        IndexFileDeleter deleterWithProbe = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            probe,
            Map.of(),
            List.of(cs1b),
            null,
            null
        );

        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "b.parquet")), commitUserData(200, 200, "uuid"));
        deleterWithProbe.addFileReferences(cs2);
        deleterWithProbe.removeFileReferences(cs1b);

        assertTrue("a.parquet should be deleted", probe.deletedFiles.contains("a.parquet"));
        for (Boolean held : probe.lockHeldDuringDelete) {
            assertFalse("Monitor should NOT be held during removeFileReferences I/O", held);
        }
    }

    // ---- Fix #14: isCommitManagedFile is now abstract ----

    public void testOrphanScanRespectsCommitManagedFiles() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();

        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        Path shardDir = tempDir.resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        Files.createDirectories(shardDir);

        Path parquetDir = shardDir.resolve("parquet");
        Files.createDirectories(parquetDir);
        Files.createFile(parquetDir.resolve("known.parquet"));
        Files.createFile(parquetDir.resolve("orphan.parquet"));
        Files.createFile(parquetDir.resolve("segments_1"));

        ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);

        // CommitFileManager that protects segments_* files
        CommitFileManager commitMgr = new CommitFileManager() {
            @Override
            public void deleteCommit(CatalogSnapshot snapshot) {}

            @Override
            public boolean isCommitManagedFile(String fileName) {
                return fileName.startsWith("segments_");
            }

            @Override
            public byte[] serializeToCommitFormat(CatalogSnapshot snapshot) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "known.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            tracker,
            Map.of(),
            List.of(cs1),
            shardPath,
            commitMgr
        );

        // orphan.parquet should be deleted (not referenced, not commit-managed)
        assertTrue("orphan.parquet should be deleted", tracker.hasDeletedFile("orphan.parquet"));
        // segments_1 should NOT be deleted (commit-managed)
        assertFalse("segments_1 should be protected by CommitFileManager", tracker.hasDeletedFile("segments_1"));
        // known.parquet should NOT be deleted (referenced)
        assertFalse("known.parquet should not be deleted", tracker.hasDeletedFile("known.parquet"));
    }

    public void testOrphanScanWithNullCommitFileManagerDeletesEverythingUnreferenced() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();

        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        Path shardDir = tempDir.resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        Files.createDirectories(shardDir);

        Path parquetDir = shardDir.resolve("parquet");
        Files.createDirectories(parquetDir);
        Files.createFile(parquetDir.resolve("known.parquet"));
        Files.createFile(parquetDir.resolve("segments_1"));

        ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "known.parquet")), commitUserData(100, 100, "uuid"));

        // null commitFileManager — no protection for commit files
        IndexFileDeleter deleter = new IndexFileDeleter(
            CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
            tracker,
            Map.of(),
            List.of(cs1),
            shardPath,
            null
        );

        // With null CommitFileManager, segments_1 is treated as an orphan
        assertTrue("segments_1 should be deleted when no CommitFileManager protects it", tracker.hasDeletedFile("segments_1"));
        assertFalse("known.parquet should not be deleted", tracker.hasDeletedFile("known.parquet"));
    }
}
