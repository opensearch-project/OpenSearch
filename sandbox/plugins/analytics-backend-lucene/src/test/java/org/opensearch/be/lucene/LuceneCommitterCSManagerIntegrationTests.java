/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.concurrent.GatedConditionalCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY;

/**
 * Integration tests wiring a real {@link LuceneCommitter} with
 * {@link CatalogSnapshotManager} and {@link CombinedCatalogSnapshotDeletionPolicy}.
 *
 * <p>Directory layout mirrors production:
 * <pre>
 *   shardPath/
 *     index/       ← Lucene directory (segments_N + real .cfs/.cfe/.si files)
 *     parquet/     ← parquet data files
 * </pre>
 *
 * <p>Real Lucene documents are ingested so segments_N references real segment files.
 * Both lucene-format and parquet-format files are tracked in CatalogSnapshot
 * and managed by our IndexFileDeleter.
 */
public class LuceneCommitterCSManagerIntegrationTests extends OpenSearchTestCase {

    private static final String LUCENE_FORMAT = "lucene";
    private static final String PARQUET_FORMAT = "parquet";
    private static final String TRANSLOG_UUID = "test-translog-uuid";
    private int docCounter = 0;

    // ---- TestEnv ----

    private record TestEnv(LuceneCommitter committer, Store store, ShardPath shardPath, Path indexDir, Path parquetDir)
        implements
            AutoCloseable {
        @Override
        public void close() throws Exception {
            committer.close();
            store.close();
        }
    }

    private TestEnv createTestEnv() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        Path indexDir = dataPath.resolve("index");
        Files.createDirectories(indexDir);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        Store store = new Store(
            shardId,
            indexSettings,
            new NIOFSDirectory(indexDir),
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath
        );
        LuceneCommitter committer = new LuceneCommitter(new CommitterConfig(indexSettings, null, store, KEEP_LATEST_ONLY));
        Path parquetDir = dataPath.resolve(PARQUET_FORMAT);
        Files.createDirectories(parquetDir);
        return new TestEnv(committer, store, shardPath, indexDir, parquetDir);
    }

    // ---- Ingest helpers ----

    /**
     * Ingests a real Lucene document, then force-merges all segments into one.
     * Returns only the merged segment's files — ensuring each CatalogSnapshot's
     * lucene files are self-contained with no cross-snapshot references.
     */
    private Set<String> ingestLuceneDocs(LuceneCommitter committer, Store store) throws IOException {
        IndexWriter iw = committer.getIndexWriter();
        iw.addDocument(List.of(new StringField("id", "doc_" + docCounter++, Field.Store.NO)));
        Set<String> filesBeforeMerge = Set.of(store.directory().listAll());
        iw.forceMerge(1);
        iw.flush();
        Set<String> filesAfterMerge = Set.of(store.directory().listAll());
        Set<String> mergedFiles = new HashSet<>(filesAfterMerge);
        mergedFiles.removeAll(filesBeforeMerge);
        mergedFiles.removeIf(f -> f.startsWith("segments") || f.equals("write.lock"));
        return mergedFiles;
    }

    /** Creates dummy parquet files on disk. */
    private Set<String> ingestParquetFiles(Path parquetDir, String... fileNames) throws IOException {
        for (String f : fileNames) {
            if (Files.exists(parquetDir.resolve(f)) == false) {
                Files.createFile(parquetDir.resolve(f));
            }
        }
        return Set.of(fileNames);
    }

    /** Builds a multi-format Segment from ingested lucene and parquet files. */
    private Segment buildSegment(long gen, Set<String> luceneFiles, Path indexDir, Set<String> parquetFiles, Path parquetDir) {
        long numRows = 100;
        WriterFileSet luceneWfs = new WriterFileSet(indexDir.toString(), gen, luceneFiles, numRows);
        WriterFileSet parquetWfs = new WriterFileSet(parquetDir.toString(), gen, parquetFiles, numRows);
        return new Segment(gen, Map.of(LUCENE_FORMAT, luceneWfs, PARQUET_FORMAT, parquetWfs));
    }

    // ---- Commit helpers ----

    private static Map<String, String> commitUserData(long maxSeqNo, long localCheckpoint) {
        Map<String, String> ud = new HashMap<>();
        ud.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        ud.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
        ud.put(Translog.TRANSLOG_UUID_KEY, TRANSLOG_UUID);
        return ud;
    }

    private static FileDeleter fileDeleterFor(Path dir) {
        return filesToDelete -> {
            for (Collection<String> files : filesToDelete.values()) {
                for (String file : files) {
                    Files.deleteIfExists(dir.resolve(file));
                }
            }
        };
    }

    private boolean fileExists(Path dir, String fileName) {
        return Files.exists(dir.resolve(fileName));
    }

    private int countLuceneCommits(Store store) throws IOException {
        return DirectoryReader.listCommits(store.directory()).size();
    }

    private void doFlush(CatalogSnapshotManager manager, LuceneCommitter committer, long maxSeqNo, long localCP) throws IOException {
        try (GatedConditionalCloseable<CatalogSnapshot> handle = manager.acquireSnapshotForCommit()) {
            CatalogSnapshot snapshot = handle.get();
            snapshot.setUserData(commitUserData(maxSeqNo, localCP));
            Map<String, String> cd = new HashMap<>(snapshot.getUserData());
            cd.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());
            committer.commit(cd);
            handle.markSuccess();
        }
    }

    private CatalogSnapshotManager bootstrap(
        TestEnv env,
        List<Segment> segments,
        long maxSeqNo,
        long localCP,
        CombinedCatalogSnapshotDeletionPolicy policy
    ) throws IOException {
        CatalogSnapshot initial = CatalogSnapshotManager.createInitialSnapshot(1L, 1L, 0L, segments, 1L, commitUserData(maxSeqNo, localCP));
        Map<String, String> cd = new HashMap<>(initial.getUserData());
        cd.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, initial.serializeToString());
        env.committer.commit(cd);
        return new CatalogSnapshotManager(
            env.committer.listCommittedSnapshots(),
            policy,
            Map.of(PARQUET_FORMAT, fileDeleterFor(env.parquetDir), LUCENE_FORMAT, fileDeleterFor(env.indexDir)),
            Map.of(),
            List.of(),
            env.shardPath,
            env.committer
        );
    }

    // ---- Test 1: Multiple ingest→merge→flush cycles with safe commit progression ----

    public void testMultipleFlushCyclesWithSafeCommitProgression() throws Exception {
        try (TestEnv env = createTestEnv()) {
            AtomicLong globalCP = new AtomicLong(100);
            var policy = new CombinedCatalogSnapshotDeletionPolicy(logger, new DefaultTranslogDeletionPolicy(-1, -1, 0), globalCP::get);

            // --- CS1: ingest, bootstrap, flush (lcp=100, globalCP=100) ---
            Set<String> lucene0 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet0 = ingestParquetFiles(env.parquetDir, "_0.parquet");
            CatalogSnapshotManager manager = bootstrap(
                env,
                List.of(buildSegment(0, lucene0, env.indexDir, parquet0, env.parquetDir)),
                100,
                100,
                policy
            );

            // --- CS2: ingest, merge, refresh, flush (lcp=200, globalCP=100) ---
            Set<String> lucene1 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet1 = ingestParquetFiles(env.parquetDir, "_1.parquet");
            manager.commitNewSnapshot(List.of(buildSegment(1, lucene1, env.indexDir, parquet1, env.parquetDir)));
            doFlush(manager, env.committer, 200, 200);

            // --- CS3: ingest, merge, refresh, flush (lcp=300, globalCP=100) ---
            Set<String> lucene2 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet2 = ingestParquetFiles(env.parquetDir, "_2.parquet");
            manager.commitNewSnapshot(List.of(buildSegment(2, lucene2, env.indexDir, parquet2, env.parquetDir)));
            doFlush(manager, env.committer, 300, 300);

            // All 3 commits: CS1=safe, CS2 in between, CS3=last
            assertEquals("CS1 + CS2 + CS3", 3, countLuceneCommits(env.store));
            for (String f : parquet0)
                assertTrue(fileExists(env.parquetDir, f));
            for (String f : parquet1)
                assertTrue(fileExists(env.parquetDir, f));
            for (String f : parquet2)
                assertTrue(fileExists(env.parquetDir, f));
            for (String f : lucene0)
                assertTrue(fileExists(env.indexDir, f));
            for (String f : lucene1)
                assertTrue(fileExists(env.indexDir, f));
            for (String f : lucene2)
                assertTrue(fileExists(env.indexDir, f));

            // --- Advance globalCP to 200, re-flush to trigger policy: CS1 deleted ---
            globalCP.set(200);
            doFlush(manager, env.committer, 300, 300);

            // CS1 deleted (lcp=100 < safe). CS2=safe(200<=200).
            for (String f : parquet0)
                assertFalse("parquet0 deleted: " + f, fileExists(env.parquetDir, f));
            for (String f : lucene0)
                assertFalse("lucene0 deleted: " + f, fileExists(env.indexDir, f));
            for (String f : parquet1)
                assertTrue("parquet1 alive (safe): " + f, fileExists(env.parquetDir, f));
            for (String f : lucene1)
                assertTrue("lucene1 alive (safe): " + f, fileExists(env.indexDir, f));
            for (String f : parquet2)
                assertTrue("parquet2 alive: " + f, fileExists(env.parquetDir, f));
            for (String f : lucene2)
                assertTrue("lucene2 alive: " + f, fileExists(env.indexDir, f));

            // --- Advance globalCP to 300, re-flush: CS2 deleted, only CS3 remains ---
            globalCP.set(300);
            doFlush(manager, env.committer, 300, 300);

            for (String f : parquet1)
                assertFalse("parquet1 deleted: " + f, fileExists(env.parquetDir, f));
            for (String f : lucene1)
                assertFalse("lucene1 deleted: " + f, fileExists(env.indexDir, f));
            for (String f : parquet2)
                assertTrue("parquet2 alive: " + f, fileExists(env.parquetDir, f));
            for (String f : lucene2)
                assertTrue("lucene2 alive: " + f, fileExists(env.indexDir, f));

            manager.close();
        }
    }

    // ---- Test 2: Shared files preserved when referencing commit exists ----

    public void testRefCountingPreservesSharedFilesAcrossCommits() throws Exception {
        try (TestEnv env = createTestEnv()) {
            AtomicLong globalCP = new AtomicLong(100);
            var policy = new CombinedCatalogSnapshotDeletionPolicy(logger, new DefaultTranslogDeletionPolicy(-1, -1, 0), globalCP::get);

            Set<String> lucene0 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet0 = ingestParquetFiles(env.parquetDir, "_0.parquet");
            CatalogSnapshotManager manager = bootstrap(
                env,
                List.of(buildSegment(0, lucene0, env.indexDir, parquet0, env.parquetDir)),
                100,
                100,
                policy
            );

            // Refresh: CS2 keeps seg0, adds seg1
            Set<String> lucene1 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet1 = ingestParquetFiles(env.parquetDir, "_1.parquet");
            manager.commitNewSnapshot(
                List.of(
                    buildSegment(0, lucene0, env.indexDir, parquet0, env.parquetDir),
                    buildSegment(1, lucene1, env.indexDir, parquet1, env.parquetDir)
                )
            );

            // Flush CS2 with globalCP still at 100 — CS1 is safe
            doFlush(manager, env.committer, 200, 200);
            assertEquals("CS1=safe, CS2=last", 2, countLuceneCommits(env.store));
            for (String f : lucene0)
                assertTrue(fileExists(env.indexDir, f));
            for (String f : parquet0)
                assertTrue(fileExists(env.parquetDir, f));

            // Advance globalCP, refresh CS3, flush
            globalCP.set(200);
            Set<String> lucene2 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet2 = ingestParquetFiles(env.parquetDir, "_2.parquet");
            manager.commitNewSnapshot(List.of(buildSegment(2, lucene2, env.indexDir, parquet2, env.parquetDir)));
            doFlush(manager, env.committer, 300, 300);

            assertEquals("CS2 (safe) + CS3 (last)", 2, countLuceneCommits(env.store));
            // seg0 files alive via safe commit CS2
            for (String f : lucene0)
                assertTrue("alive via safe: " + f, fileExists(env.indexDir, f));
            for (String f : parquet0)
                assertTrue("alive via safe: " + f, fileExists(env.parquetDir, f));
            for (String f : lucene2)
                assertTrue(fileExists(env.indexDir, f));
            for (String f : parquet2)
                assertTrue(fileExists(env.parquetDir, f));

            manager.close();
        }
    }

    // ---- Test 3: Snapshot protection preserves files across both formats ----

    public void testSnapshotProtectionPreservesAllFiles() throws Exception {
        try (TestEnv env = createTestEnv()) {
            AtomicLong globalCP = new AtomicLong(100);
            var policy = new CombinedCatalogSnapshotDeletionPolicy(logger, new DefaultTranslogDeletionPolicy(-1, -1, 0), globalCP::get);

            Set<String> lucene0 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet0 = ingestParquetFiles(env.parquetDir, "_0.parquet");
            CatalogSnapshotManager manager = bootstrap(
                env,
                List.of(buildSegment(0, lucene0, env.indexDir, parquet0, env.parquetDir)),
                100,
                100,
                policy
            );

            // Hold CS1
            GatedCloseable<CatalogSnapshot> held = manager.acquireCommittedSnapshot(false);

            // Refresh: CS2
            Set<String> lucene1 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet1 = ingestParquetFiles(env.parquetDir, "_1.parquet");
            manager.commitNewSnapshot(List.of(buildSegment(1, lucene1, env.indexDir, parquet1, env.parquetDir)));

            globalCP.set(200);
            doFlush(manager, env.committer, 200, 200);

            // CS1 protected
            for (String f : lucene0)
                assertTrue("protected: " + f, fileExists(env.indexDir, f));
            for (String f : parquet0)
                assertTrue("protected: " + f, fileExists(env.parquetDir, f));
            assertTrue("CS1's commit still exists", countLuceneCommits(env.store) >= 2);

            // Release
            held.close();

            for (String f : lucene0)
                assertFalse("deleted: " + f, fileExists(env.indexDir, f));
            for (String f : parquet0)
                assertFalse("deleted: " + f, fileExists(env.parquetDir, f));
            assertEquals("Only CS2's commit remains", 1, countLuceneCommits(env.store));
            for (String f : lucene1)
                assertTrue(fileExists(env.indexDir, f));
            for (String f : parquet1)
                assertTrue(fileExists(env.parquetDir, f));

            manager.close();
        }
    }

    // ---- Test 4: Recovery trims unsafe commits, cleans orphans ----

    public void testRecoveryAfterCrashTrimsUnsafeCommits() throws Exception {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        Path indexDir = dataPath.resolve("index");
        Files.createDirectories(indexDir);
        Path parquetDir = dataPath.resolve(PARQUET_FORMAT);
        Files.createDirectories(parquetDir);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        Set<String> lucene0, lucene1, lucene2;

        // Phase 1: Pre-crash — 3 commits
        {
            Store store = new Store(
                shardId,
                indexSettings,
                new NIOFSDirectory(indexDir),
                new DummyShardLock(shardId),
                Store.OnClose.EMPTY,
                shardPath
            );
            LuceneCommitter committer = new LuceneCommitter(new CommitterConfig(indexSettings, null, store, KEEP_LATEST_ONLY));

            lucene0 = ingestLuceneDocs(committer, store);
            Set<String> parquet0 = ingestParquetFiles(parquetDir, "_0.parquet");
            CatalogSnapshot cs1 = CatalogSnapshotManager.createInitialSnapshot(
                1L,
                1L,
                0L,
                List.of(buildSegment(0, lucene0, indexDir, parquet0, parquetDir)),
                1L,
                commitUserData(100, 100)
            );
            Map<String, String> cd1 = new HashMap<>(cs1.getUserData());
            cd1.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs1.serializeToString());
            committer.commit(cd1);

            lucene1 = ingestLuceneDocs(committer, store);
            Set<String> parquet1 = ingestParquetFiles(parquetDir, "_1.parquet");
            CatalogSnapshot cs2 = CatalogSnapshotManager.createInitialSnapshot(
                2L,
                2L,
                0L,
                List.of(buildSegment(1, lucene1, indexDir, parquet1, parquetDir)),
                2L,
                commitUserData(200, 200)
            );
            Map<String, String> cd2 = new HashMap<>(cs2.getUserData());
            cd2.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs2.serializeToString());
            committer.commit(cd2);

            lucene2 = ingestLuceneDocs(committer, store);
            Set<String> parquet2 = ingestParquetFiles(parquetDir, "_2.parquet");
            CatalogSnapshot cs3 = CatalogSnapshotManager.createInitialSnapshot(
                3L,
                3L,
                0L,
                List.of(buildSegment(2, lucene2, indexDir, parquet2, parquetDir)),
                3L,
                commitUserData(300, 300)
            );
            Map<String, String> cd3 = new HashMap<>(cs3.getUserData());
            cd3.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs3.serializeToString());
            committer.commit(cd3);

            assertEquals(3, DirectoryReader.listCommits(store.directory()).size());
            committer.close();
            store.close();
        }

        // Phase 2: Recovery — globalCP=100
        {
            AtomicLong globalCP = new AtomicLong(100);
            var policy = new CombinedCatalogSnapshotDeletionPolicy(logger, new DefaultTranslogDeletionPolicy(-1, -1, 0), globalCP::get);
            Store store = new Store(
                shardId,
                indexSettings,
                new NIOFSDirectory(indexDir),
                new DummyShardLock(shardId),
                Store.OnClose.EMPTY,
                shardPath
            );
            LuceneCommitter committer = new LuceneCommitter(new CommitterConfig(indexSettings, null, store, policy));

            assertEquals("Only safe commit remains", 1, DirectoryReader.listCommits(store.directory()).size());

            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                committer.listCommittedSnapshots(),
                policy,
                Map.of(PARQUET_FORMAT, fileDeleterFor(parquetDir), LUCENE_FORMAT, fileDeleterFor(indexDir)),
                Map.of(),
                List.of(),
                shardPath,
                committer
            );

            // CS1's files survive
            for (String f : lucene0)
                assertTrue("lucene0 survives: " + f, fileExists(indexDir, f));
            assertTrue("_0.parquet survives", fileExists(parquetDir, "_0.parquet"));

            // CS2/CS3 orphan parquet files cleaned up
            assertFalse("_1.parquet orphan", fileExists(parquetDir, "_1.parquet"));
            assertFalse("_2.parquet orphan", fileExists(parquetDir, "_2.parquet"));

            manager.close();
            committer.close();
            store.close();
        }
    }

    // ---- Test 5: Recovery then normal operation ----

    public void testRecoveryThenNormalOperationWorks() throws Exception {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        Path indexDir = dataPath.resolve("index");
        Files.createDirectories(indexDir);
        Path parquetDir = dataPath.resolve(PARQUET_FORMAT);
        Files.createDirectories(parquetDir);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        Set<String> lucene0;

        // Phase 1: Pre-crash — 2 commits
        {
            Store store = new Store(
                shardId,
                indexSettings,
                new NIOFSDirectory(indexDir),
                new DummyShardLock(shardId),
                Store.OnClose.EMPTY,
                shardPath
            );
            LuceneCommitter committer = new LuceneCommitter(new CommitterConfig(indexSettings, null, store, KEEP_LATEST_ONLY));

            lucene0 = ingestLuceneDocs(committer, store);
            Set<String> parquet0 = ingestParquetFiles(parquetDir, "_0.parquet");
            CatalogSnapshot cs1 = CatalogSnapshotManager.createInitialSnapshot(
                1L,
                1L,
                0L,
                List.of(buildSegment(0, lucene0, indexDir, parquet0, parquetDir)),
                1L,
                commitUserData(100, 100)
            );
            Map<String, String> cd1 = new HashMap<>(cs1.getUserData());
            cd1.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs1.serializeToString());
            committer.commit(cd1);

            Set<String> lucene1 = ingestLuceneDocs(committer, store);
            Set<String> parquet1 = ingestParquetFiles(parquetDir, "_1.parquet");
            CatalogSnapshot cs2 = CatalogSnapshotManager.createInitialSnapshot(
                2L,
                2L,
                0L,
                List.of(buildSegment(1, lucene1, indexDir, parquet1, parquetDir)),
                2L,
                commitUserData(200, 200)
            );
            Map<String, String> cd2 = new HashMap<>(cs2.getUserData());
            cd2.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs2.serializeToString());
            committer.commit(cd2);

            committer.close();
            store.close();
        }

        // Phase 2: Recovery + normal operation
        {
            AtomicLong globalCP = new AtomicLong(100);
            var policy = new CombinedCatalogSnapshotDeletionPolicy(logger, new DefaultTranslogDeletionPolicy(-1, -1, 0), globalCP::get);
            Store store = new Store(
                shardId,
                indexSettings,
                new NIOFSDirectory(indexDir),
                new DummyShardLock(shardId),
                Store.OnClose.EMPTY,
                shardPath
            );
            LuceneCommitter committer = new LuceneCommitter(new CommitterConfig(indexSettings, null, store, policy));

            assertEquals(1, DirectoryReader.listCommits(store.directory()).size());
            for (String f : lucene0)
                assertTrue("lucene0 survives: " + f, fileExists(indexDir, f));
            assertTrue(fileExists(parquetDir, "_0.parquet"));

            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                committer.listCommittedSnapshots(),
                policy,
                Map.of(PARQUET_FORMAT, fileDeleterFor(parquetDir), LUCENE_FORMAT, fileDeleterFor(indexDir)),
                Map.of(),
                List.of(),
                shardPath,
                committer
            );

            assertFalse("_1.parquet orphan deleted", fileExists(parquetDir, "_1.parquet"));

            // Normal operation: ingest + refresh + flush
            Set<String> lucene3 = ingestLuceneDocs(committer, store);
            Set<String> parquet3 = ingestParquetFiles(parquetDir, "_3.parquet");
            manager.commitNewSnapshot(List.of(buildSegment(3, lucene3, indexDir, parquet3, parquetDir)));

            globalCP.set(300);
            doFlush(manager, committer, 300, 300);

            assertEquals("Only CS3's commit remains", 1, DirectoryReader.listCommits(store.directory()).size());
            for (String f : lucene0)
                assertFalse("lucene0 deleted: " + f, fileExists(indexDir, f));
            assertFalse("_0.parquet deleted", fileExists(parquetDir, "_0.parquet"));
            for (String f : lucene3)
                assertTrue(fileExists(indexDir, f));
            assertTrue(fileExists(parquetDir, "_3.parquet"));

            manager.close();
            committer.close();
            store.close();
        }
    }
}
