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
        WriterFileSet wfs = new WriterFileSet("/data", gen, Set.of(files), files.length);
        return new Segment(gen, Map.of(format, wfs));
    }

    private static CatalogSnapshot snapshot(long gen, List<Segment> segments, Map<String, String> userData) {
        return new DataformatAwareCatalogSnapshot(gen, gen, 0L, segments, 0L, userData);
    }

    static class TrackingFileDeleter implements FileDeleter {
        final List<Map<String, Collection<String>>> deletions = new ArrayList<>();

        @Override
        public void deleteFiles(Map<String, Collection<String>> filesToDelete) {
            deletions.add(new HashMap<>(filesToDelete));
        }

        Set<String> allDeletedFiles(String format) {
            Set<String> result = new HashSet<>();
            for (Map<String, Collection<String>> deletion : deletions) {
                Collection<String> files = deletion.get(format);
                if (files != null) {
                    result.addAll(files);
                }
            }
            return result;
        }

        boolean hasDeletedFile(String format, String file) {
            return allDeletedFiles(format).contains(file);
        }
    }

    // ---- addFileReferences / removeFileReferences ----

    public void testAddFileReferencesTracksNewFiles() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet", "b.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY, tracker, cs1, null);

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

        IndexFileDeleter deleter = new IndexFileDeleter(CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY, tracker, cs1, null);

        // Add cs2 sharing a.parquet but not b.parquet
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "a.parquet", "c.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);

        // Remove cs1's refs
        deleter.removeFileReferences(cs1);

        // b.parquet was only in cs1 → deleted
        assertTrue(tracker.hasDeletedFile("parquet", "b.parquet"));
        // a.parquet shared with cs2 → NOT deleted
        assertFalse(tracker.hasDeletedFile("parquet", "a.parquet"));
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

        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, cs1, null);

        // Refresh: cs2 with merged files
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(2, "parquet", "new_merged.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);

        // Simulate manager releasing cs1 (as commitNewSnapshot would)
        cs1.decRef();

        // Commit cs2, advance globalCP
        globalCP.set(200);
        deleter.onCommit(cs2);

        // cs1 deleted by policy → old_a and old_b should be deleted
        assertTrue(tracker.hasDeletedFile("parquet", "old_a.parquet"));
        assertTrue(tracker.hasDeletedFile("parquet", "old_b.parquet"));
        assertFalse(tracker.hasDeletedFile("parquet", "new_merged.parquet"));
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

        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, cs1, null);

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
        assertTrue(tracker.hasDeletedFile("parquet", "only_cs1.parquet"));
        assertFalse(tracker.hasDeletedFile("parquet", "shared.parquet"));
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
        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, cs1, null);

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
        assertFalse(tracker.hasDeletedFile("parquet", "cs1_file.parquet"));

        // Release hold
        held.close();

        // revisitPolicy should now delete cs1
        deleter.revisitPolicy();
        assertTrue(tracker.hasDeletedFile("parquet", "cs1_file.parquet"));
    }

    // ---- Multi-format ----

    public void testMultiFormatFileDeletion() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
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
                new WriterFileSet("/data", 0, Set.of("data.parquet"), 1),
                "lucene",
                new WriterFileSet("/data", 0, Set.of("_0.cfs"), 1)
            )
        );
        CatalogSnapshot cs1 = snapshot(1, List.of(seg), commitUserData(100, 100, "uuid"));
        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, cs1, null);

        // cs2 has completely different files
        Segment seg2 = new Segment(
            1,
            Map.of(
                "parquet",
                new WriterFileSet("/data", 1, Set.of("data2.parquet"), 1),
                "lucene",
                new WriterFileSet("/data", 1, Set.of("_1.cfs"), 1)
            )
        );
        CatalogSnapshot cs2 = snapshot(2, List.of(seg2), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);

        // Simulate manager releasing cs1
        cs1.decRef();

        globalCP.set(200);
        deleter.onCommit(cs2);

        // Both formats' old files should be deleted
        assertTrue(tracker.hasDeletedFile("parquet", "data.parquet"));
        assertTrue(tracker.hasDeletedFile("lucene", "_0.cfs"));
        assertFalse(tracker.hasDeletedFile("parquet", "data2.parquet"));
        assertFalse(tracker.hasDeletedFile("lucene", "_1.cfs"));
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
        IndexFileDeleter deleter = new IndexFileDeleter(policy, tracker, cs1, null);

        // cs1 has refCount=2 (manager + commit from constructor)
        assertEquals(2, cs1.refCount());

        // Simulate manager releasing its ref (like commitNewSnapshot replacing it)
        cs1.decRef(); // manager releases → refCount=1 (commit still holds)
        assertEquals(1, cs1.refCount());

        // Files NOT deleted — commit ref keeps it alive
        assertTrue(tracker.deletions.isEmpty());

        // Add cs2 and commit
        CatalogSnapshot cs2 = snapshot(2, List.of(segment(1, "parquet", "cs2.parquet")), commitUserData(200, 200, "uuid"));
        deleter.addFileReferences(cs2);
        globalCP.set(200);
        deleter.onCommit(cs2);

        // Policy deletes cs1 → decRef → refCount=0 → removeFileReferences
        assertEquals(0, cs1.refCount());
        assertTrue(tracker.hasDeletedFile("parquet", "cs1.parquet"));
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

        IndexFileDeleter deleter = new IndexFileDeleter(CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY, tracker, cs1, shardPath);

        assertTrue(tracker.hasDeletedFile("parquet", "orphan1.parquet"));
        assertTrue(tracker.hasDeletedFile("parquet", "orphan2.parquet"));
        assertFalse(tracker.hasDeletedFile("parquet", "known.parquet"));
    }

    public void testDeleteOrphanedFilesSkipsMissingDirectory() throws IOException {
        TrackingFileDeleter tracker = new TrackingFileDeleter();
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        Path shardDir = tempDir.resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        Files.createDirectories(shardDir);

        ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);

        CatalogSnapshot cs1 = snapshot(1, List.of(segment(0, "parquet", "a.parquet")), commitUserData(100, 100, "uuid"));

        IndexFileDeleter deleter = new IndexFileDeleter(CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY, tracker, cs1, shardPath);
        assertTrue(tracker.deletions.isEmpty());
    }
}
