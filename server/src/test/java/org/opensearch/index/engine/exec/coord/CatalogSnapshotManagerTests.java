/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.concurrent.GatedBiCloseable;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for {@link CatalogSnapshotManager}.
 */
public class CatalogSnapshotManagerTests extends OpenSearchTestCase {

    public void testCommitProducesCorrectNewSnapshot() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                long previousGeneration;
                Set<Long> seenIds = new HashSet<>();
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    previousGeneration = ref.get().getGeneration();
                    seenIds.add(ref.get().getId());
                }

                int numCommits = randomIntBetween(1, 10);
                for (int c = 0; c < numCommits; c++) {
                    List<Segment> newSegments = randomSegments();
                    manager.commitNewSnapshot(newSegments);

                    try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                        assertEquals(previousGeneration + 1, ref.get().getGeneration());
                        assertTrue(seenIds.add(ref.get().getId()));
                        assertEquals(newSegments, ref.get().getSegments());
                        previousGeneration = ref.get().getGeneration();
                    }
                }
            } finally {
                manager.close();
            }
        }
    }

    public void testUserDataPreservationOnCommit() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            Map<String, String> initialUserData = randomUserData(randomIntBetween(1, 5));
            long initGen = randomIntBetween(0, 100);
            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                randomNonNegativeLong(),
                initGen,
                randomNonNegativeLong(),
                randomSegments(),
                randomNonNegativeLong(),
                initialUserData,
                CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
                Map.of(),
                Map.of(),
                List.of(),
                null
            );
            try {
                manager.commitNewSnapshot(randomSegments());
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    assertEquals(initialUserData, ref.get().getUserData());
                }

                manager.commitNewSnapshot(randomSegments());
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    assertEquals(initialUserData, ref.get().getUserData());
                }
            } finally {
                manager.close();
            }
        }
    }

    public void testReferenceCountingLifecycle() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            long initGen = randomIntBetween(0, 100);
            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                randomNonNegativeLong(),
                initGen,
                randomNonNegativeLong(),
                randomSegments(),
                randomNonNegativeLong(),
                Collections.emptyMap(),
                CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
                Map.of(),
                Map.of(),
                List.of(),
                null
            );

            CatalogSnapshot initialSnapshot;
            try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                initialSnapshot = ref.get();
                // refCount = 3: manager "latest" + commit ref (from IndexFileDeleter) + reader
                assertEquals(3, initialSnapshot.refCount());
            }
            // refCount = 2: manager "latest" + commit ref
            assertEquals(2, initialSnapshot.refCount());

            manager.commitNewSnapshot(randomSegments());
            // refCount = 1: commit ref still held (no flush happened to trigger deletion policy)
            assertEquals(1, initialSnapshot.refCount());

            int numCommits = randomIntBetween(1, 8);
            for (int c = 0; c < numCommits; c++) {
                CatalogSnapshot prev;
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    prev = ref.get();
                    // refCount = 2: manager "latest" + reader (no commit ref on refresh-created snapshots)
                    assertEquals(2, prev.refCount());
                }
                assertEquals(1, prev.refCount());
                manager.commitNewSnapshot(randomSegments());
                assertEquals(0, prev.refCount());
            }

            CatalogSnapshot finalSnapshot;
            try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                finalSnapshot = ref.get();
                assertEquals(2, finalSnapshot.refCount());
            }
            assertEquals(1, finalSnapshot.refCount());
            manager.close();
            assertEquals(0, finalSnapshot.refCount());
        }
    }

    public void testAcquireAndReleaseViaGatedCloseable() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                CatalogSnapshot currentSnap;
                // Initial snapshot has commit ref, so: manager + commit + reader = 3
                try (GatedCloseable<CatalogSnapshot> initialRef = manager.acquireSnapshot()) {
                    currentSnap = initialRef.get();
                    assertEquals(3, currentSnap.refCount());
                }
                // manager + commit = 2
                assertEquals(2, currentSnap.refCount());

                int numAcquires = randomIntBetween(1, 5);
                List<GatedCloseable<CatalogSnapshot>> refs = new ArrayList<>();
                for (int a = 0; a < numAcquires; a++) {
                    refs.add(manager.acquireSnapshot());
                    assertEquals(2 + (a + 1), currentSnap.refCount());
                }
                for (int r = 0; r < numAcquires; r++) {
                    refs.get(r).close();
                    assertEquals(2 + numAcquires - r - 1, currentSnap.refCount());
                }
                assertEquals(2, currentSnap.refCount());

                // Acquire a new ref, then replace via commitNewSnapshot
                GatedCloseable<CatalogSnapshot> heldRef = manager.acquireSnapshot();
                CatalogSnapshot heldSnapshot = heldRef.get();
                // manager + commit + reader = 3
                assertEquals(3, heldSnapshot.refCount());

                manager.commitNewSnapshot(randomSegments());
                // commit + reader = 2 (manager released its ref)
                assertEquals(2, heldSnapshot.refCount());

                heldRef.close();
                // commit = 1 (reader released, commit ref still held — no flush to trigger policy)
                assertEquals(1, heldSnapshot.refCount());
            } finally {
                manager.close();
            }
        }
    }

    public void testClosedManagerRejectsAcquisition() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            for (int c = 0; c < randomIntBetween(0, 5); c++) {
                manager.commitNewSnapshot(randomSegments());
            }
            manager.close();
            expectThrows(IllegalStateException.class, manager::acquireSnapshot);
        }
    }

    public void testInitialSnapshotRecovery() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            long id = randomNonNegativeLong();
            long generation = randomIntBetween(0, 100);
            long version = randomNonNegativeLong();
            long lastWriterGeneration = randomNonNegativeLong();
            List<Segment> segments = randomIntBetween(1, 5) == 1 ? Collections.emptyList() : randomSegments();
            Map<String, String> userData = randomUserData(randomIntBetween(0, 4));

            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                id,
                generation,
                version,
                segments,
                lastWriterGeneration,
                userData,
                CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
                Map.of(),
                Map.of(),
                List.of(),
                null
            );
            try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                CatalogSnapshot acquired = ref.get();
                assertEquals(id, acquired.getId());
                assertEquals(generation, acquired.getGeneration());
                assertEquals(segments, acquired.getSegments());
                assertEquals(userData, acquired.getUserData());
                assertEquals(lastWriterGeneration, acquired.getLastWriterGeneration());
            } finally {
                manager.close();
            }
        }
    }

    public void testCloseInternalInvokedOnCommit() throws Exception {
        CatalogSnapshotManager manager = createRandomManager();

        // Do a refresh so we get a snapshot without a commit ref
        manager.commitNewSnapshot(randomSegments());

        CatalogSnapshot refreshedSnapshot;
        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            refreshedSnapshot = ref.get();
        }
        assertFalse(((DataformatAwareCatalogSnapshot) refreshedSnapshot).isClosed());

        manager.commitNewSnapshot(randomSegments());
        assertTrue(
            "snapshot should be closed when commit replaces the last ref",
            ((DataformatAwareCatalogSnapshot) refreshedSnapshot).isClosed()
        );
        manager.close();
    }

    public void testCloseInternalInvokedOnManagerClose() throws Exception {
        CatalogSnapshotManager manager = createRandomManager();

        // Do a refresh so we get a snapshot without a commit ref
        manager.commitNewSnapshot(randomSegments());

        CatalogSnapshot snapshot;
        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            snapshot = ref.get();
        }
        assertFalse(((DataformatAwareCatalogSnapshot) snapshot).isClosed());

        manager.close();
        assertTrue("snapshot should be closed when manager releases the last ref", ((DataformatAwareCatalogSnapshot) snapshot).isClosed());
    }

    public void testCloseInternalNotInvokedWhileRefsHeld() throws Exception {
        CatalogSnapshotManager manager = createRandomManager();

        // Do a refresh so we get a snapshot without a commit ref
        manager.commitNewSnapshot(randomSegments());

        GatedCloseable<CatalogSnapshot> heldRef = manager.acquireSnapshot();
        CatalogSnapshot heldSnapshot = heldRef.get();
        assertFalse(((DataformatAwareCatalogSnapshot) heldSnapshot).isClosed());

        manager.commitNewSnapshot(randomSegments());
        assertFalse("snapshot should not be closed while a ref is still held", ((DataformatAwareCatalogSnapshot) heldSnapshot).isClosed());

        heldRef.close();
        assertTrue("snapshot should be closed after the last ref is released", ((DataformatAwareCatalogSnapshot) heldSnapshot).isClosed());

        manager.close();
    }

    // --- File deletion and commit lifecycle tests ---

    /**
     * Tracks files deleted via FileDeleter for assertions.
     */
    private static class TrackingFileDeleter implements FileDeleter {
        final Set<String> deletedFiles = new HashSet<>();

        @Override
        public void deleteFiles(Map<String, Collection<String>> filesToDelete) {
            for (Collection<String> files : filesToDelete.values()) {
                deletedFiles.addAll(files);
            }
        }
    }

    private static Map<String, String> commitUserData(long maxSeqNo, long localCheckpoint, String translogUUID) {
        Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
        userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
        return userData;
    }

    private static Segment segment(long gen, String format, String... files) {
        WriterFileSet wfs = new WriterFileSet("/data", gen, Set.of(files), files.length);
        return new Segment(gen, Map.of(format, wfs));
    }

    /**
     * Trace from the dry-run: refresh adds new segment, flush commits,
     * old committed snapshot's files are deleted when policy removes it.
     */
    public void testRefreshThenFlushDeletesOldCommitFiles() throws Exception {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);
        String translogUUID = "test-uuid";

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        // CS1: initial committed snapshot with segments _0, _1
        List<Segment> cs1Segments = List.of(
            segment(0, "parquet", "_0_data.parquet", "_0_index.parquet"),
            segment(1, "parquet", "_1_data.parquet", "_1_index.parquet")
        );
        Map<String, String> userData = commitUserData(100, 100, translogUUID);

        CatalogSnapshotManager manager = new CatalogSnapshotManager(
            1L,
            1L,
            0L,
            cs1Segments,
            1L,
            userData,
            policy,
            Map.of("parquet", tracker),
            Map.of(),
            List.of(),
            null
        );

        // Refresh: CS2 adds segment _2, keeps _0 and _1
        List<Segment> cs2Segments = List.of(
            segment(0, "parquet", "_0_data.parquet", "_0_index.parquet"),
            segment(1, "parquet", "_1_data.parquet", "_1_index.parquet"),
            segment(2, "parquet", "_2_data.parquet", "_2_index.parquet")
        );
        manager.commitNewSnapshot(cs2Segments);

        // No files deleted yet — CS1's commit ref keeps its files alive
        assertTrue(tracker.deletedFiles.isEmpty());

        // Flush: CS2 becomes a commit, policy deletes CS1
        globalCP.set(200);
        try (GatedBiCloseable<CatalogSnapshot> commitRef = manager.acquireSnapshotForCommit()) {
            // set userData on the snapshot for the policy to read
            commitRef.get().setUserData(commitUserData(200, 200, translogUUID));
            commitRef.markSuccess();
        }

        // CS1's files that are NOT in CS2 should be deleted
        // But all of CS1's files (_0, _1) are also in CS2, so nothing should be deleted
        assertTrue("No files should be deleted since CS2 shares all files with CS1", tracker.deletedFiles.isEmpty());

        manager.close();
    }

    /**
     * After merge, old pre-merge files are deleted when the commit referencing them is removed.
     */
    public void testMergedFilesDeletedAfterCommit() throws Exception {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);
        String translogUUID = "test-uuid";

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        // CS1: segments _0, _1
        List<Segment> cs1Segments = List.of(segment(0, "parquet", "_0_data.parquet"), segment(1, "parquet", "_1_data.parquet"));

        CatalogSnapshotManager manager = new CatalogSnapshotManager(
            1L,
            1L,
            0L,
            cs1Segments,
            1L,
            commitUserData(100, 100, translogUUID),
            policy,
            Map.of("parquet", tracker),
            Map.of(),
            List.of(),
            null
        );

        // Refresh: merge _0+_1 into _2, add new _3
        List<Segment> cs2Segments = List.of(segment(2, "parquet", "_2_data.parquet"), segment(3, "parquet", "_3_data.parquet"));
        manager.commitNewSnapshot(cs2Segments);

        // Flush CS2
        globalCP.set(200);
        try (GatedBiCloseable<CatalogSnapshot> commitRef = manager.acquireSnapshotForCommit()) {
            commitRef.get().setUserData(commitUserData(200, 200, translogUUID));
            commitRef.markSuccess();
        }

        // CS1 deleted by policy → _0_data and _1_data should be deleted (not in CS2)
        Set<String> deleted = tracker.deletedFiles;
        assertTrue("_0_data.parquet should be deleted", deleted.contains("_0_data.parquet"));
        assertTrue("_1_data.parquet should be deleted", deleted.contains("_1_data.parquet"));
        assertFalse("_2_data.parquet should NOT be deleted", deleted.contains("_2_data.parquet"));
        assertFalse("_3_data.parquet should NOT be deleted", deleted.contains("_3_data.parquet"));

        manager.close();
    }

    /**
     * Snapshot protection: a held snapshot prevents file deletion even after a new commit.
     */
    public void testSnapshotProtectionPreventsFileDeletion() throws Exception {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);
        String translogUUID = "test-uuid";

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        // CS1: segment _0
        CatalogSnapshotManager manager = new CatalogSnapshotManager(
            1L,
            1L,
            0L,
            List.of(segment(0, "parquet", "_0_data.parquet")),
            1L,
            commitUserData(100, 100, translogUUID),
            policy,
            Map.of("parquet", tracker),
            Map.of(),
            List.of(),
            null
        );

        // Flush CS1 so it's a proper commit
        try (GatedBiCloseable<CatalogSnapshot> commitRef = manager.acquireSnapshotForCommit()) {
            commitRef.get().setUserData(commitUserData(100, 100, translogUUID));
            commitRef.markSuccess();
        }

        // Acquire committed snapshot (simulating _snapshot API)
        GatedCloseable<CatalogSnapshot> held = manager.acquireCommittedSnapshot(false);

        // Refresh: CS2 with different files
        manager.commitNewSnapshot(List.of(segment(1, "parquet", "_1_data.parquet")));

        // Flush CS2
        globalCP.set(200);
        try (GatedBiCloseable<CatalogSnapshot> commitRef = manager.acquireSnapshotForCommit()) {
            commitRef.get().setUserData(commitUserData(200, 200, translogUUID));
            commitRef.markSuccess();
        }

        // CS1 is snapshotted — should NOT be deleted
        assertFalse("_0_data should not be deleted while snapshot is held", tracker.deletedFiles.contains("_0_data.parquet"));

        // Release the snapshot — triggers revisitPolicy which cleans up
        held.close();

        // Now _0_data should be deleted
        assertTrue("_0_data should be deleted after snapshot release", tracker.deletedFiles.contains("_0_data.parquet"));

        manager.close();
    }

    /**
     * Reader holding a snapshot keeps it alive across refreshes,
     * files only deleted after reader releases AND commit is deleted by policy.
     */
    public void testReaderHoldsSnapshotAliveAcrossRefreshes() throws Exception {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);
        String translogUUID = "test-uuid";

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        // CS1: segment _0
        CatalogSnapshotManager manager = new CatalogSnapshotManager(
            1L,
            1L,
            0L,
            List.of(segment(0, "parquet", "_0_data.parquet")),
            1L,
            commitUserData(100, 100, translogUUID),
            policy,
            Map.of("parquet", tracker),
            Map.of(),
            List.of(),
            null
        );

        // Reader acquires CS1
        GatedCloseable<CatalogSnapshot> readerRef = manager.acquireSnapshot();

        // Multiple refreshes replace CS1 as latest
        manager.commitNewSnapshot(List.of(segment(1, "parquet", "_1_data.parquet")));
        manager.commitNewSnapshot(List.of(segment(2, "parquet", "_2_data.parquet")));

        // Flush latest
        globalCP.set(200);
        try (GatedBiCloseable<CatalogSnapshot> commitRef = manager.acquireSnapshotForCommit()) {
            commitRef.get().setUserData(commitUserData(200, 200, translogUUID));
            commitRef.markSuccess();
        }

        // CS1's commit ref was released by policy, but reader still holds a ref
        // So CS1's refCount > 0, removeFileReferences not called yet
        assertFalse("_0_data should not be deleted while reader holds ref", tracker.deletedFiles.contains("_0_data.parquet"));

        // Reader releases
        readerRef.close();

        // Now CS1 refCount hits 0 → removeFileReferences → _0_data deleted
        assertTrue("_0_data should be deleted after reader releases", tracker.deletedFiles.contains("_0_data.parquet"));

        manager.close();
    }

    /**
     * Shared files between commits: file only deleted when ALL snapshots referencing it are gone.
     */
    public void testSharedFilesDeletedOnlyWhenAllRefsGone() throws Exception {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        AtomicLong globalCP = new AtomicLong(100);
        String translogUUID = "test-uuid";

        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );

        // CS1: _0 and _1
        CatalogSnapshotManager manager = new CatalogSnapshotManager(
            1L,
            1L,
            0L,
            List.of(segment(0, "parquet", "_0_data.parquet"), segment(1, "parquet", "_shared.parquet")),
            1L,
            commitUserData(100, 100, translogUUID),
            policy,
            Map.of("parquet", tracker),
            Map.of(),
            List.of(),
            null
        );

        // Refresh CS2: keeps _shared, replaces _0 with _2
        manager.commitNewSnapshot(List.of(segment(1, "parquet", "_shared.parquet"), segment(2, "parquet", "_2_data.parquet")));

        // Flush CS2
        globalCP.set(200);
        try (GatedBiCloseable<CatalogSnapshot> commitRef = manager.acquireSnapshotForCommit()) {
            commitRef.get().setUserData(commitUserData(200, 200, translogUUID));
            commitRef.markSuccess();
        }

        // CS1 deleted → _0_data deleted, but _shared survives (still in CS2)
        Set<String> deleted = tracker.deletedFiles;
        assertTrue("_0_data should be deleted", deleted.contains("_0_data.parquet"));
        assertFalse("_shared should NOT be deleted (still in CS2)", deleted.contains("_shared.parquet"));

        // Refresh CS3: drops _shared
        manager.commitNewSnapshot(List.of(segment(3, "parquet", "_3_data.parquet")));

        // Flush CS3
        globalCP.set(300);
        try (GatedBiCloseable<CatalogSnapshot> commitRef = manager.acquireSnapshotForCommit()) {
            commitRef.get().setUserData(commitUserData(300, 300, translogUUID));
            commitRef.markSuccess();
        }

        // Now _shared should be deleted (CS2 commit deleted by policy)
        assertTrue("_shared should be deleted after CS2 commit removed", tracker.deletedFiles.contains("_shared.parquet"));

        manager.close();
    }

    // --- helpers ---

    private WriterFileSet randomWriterFileSet(String format) {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        String[] extensions = "lucene".equals(format) ? new String[] { "cfs", "si", "dat" } : new String[] { "parquet" };
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom(extensions));
        }
        return new WriterFileSet(directory, randomNonNegativeLong(), files, randomIntBetween(0, 10000));
    }

    private Segment randomSegment() {
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 2); i++) {
            String format = randomFrom("lucene", "parquet");
            dfGrouped.put(format, randomWriterFileSet(format));
        }
        return new Segment(randomNonNegativeLong(), dfGrouped);
    }

    private List<Segment> randomSegments() {
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            segments.add(randomSegment());
        }
        return segments;
    }

    private Map<String, String> randomUserData(int entries) {
        Map<String, String> userData = new HashMap<>();
        for (int i = 0; i < entries; i++) {
            userData.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        return userData;
    }

    private CatalogSnapshotManager createRandomManager() {
        try {
            return new CatalogSnapshotManager(
                randomNonNegativeLong(),
                randomIntBetween(0, 100),
                randomNonNegativeLong(),
                randomSegments(),
                randomNonNegativeLong(),
                Map.of(),
                CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
                Map.of(),
                Map.of(),
                List.of(),
                null
            );
        } catch (IOException e) {
            throw new AssertionError("unreachable", e);
        }
    }
}
