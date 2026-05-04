/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.concurrent.GatedConditionalCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
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

    private record TestEnv(LuceneCommitter committer, Store store, ShardPath shardPath, Path indexDir, Path parquetDir, Path translogDir)
        implements
            AutoCloseable {
        @Override
        public void close() throws Exception {
            committer.close();
            store.close();
        }
    }

    private static EngineConfig buildEngineConfig(IndexSettings indexSettings, Store store, ShardId shardId, Path translogPath) {
        EngineConfig.Builder builder = new EngineConfig.Builder().indexSettings(indexSettings)
            .store(store)
            .codecService(
                new CodecService(null, indexSettings, LogManager.getLogger(LuceneCommitterCSManagerIntegrationTests.class), List.of())
            )
            .retentionLeasesSupplier(() -> new RetentionLeases(0, 0, java.util.Collections.emptyList()));
        if (translogPath != null) {
            builder.translogConfig(new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false));
        }
        return builder.build();
    }

    private TestEnv createTestEnv() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        Path indexDir = dataPath.resolve("index");
        Files.createDirectories(indexDir);
        Path translogDir = dataPath.resolve("translog");
        Files.createDirectories(translogDir);
        // Create a real translog so readGlobalCheckpoint works during safe bootstrap
        Translog.createEmptyTranslog(translogDir, shardId, SequenceNumbers.NO_OPS_PERFORMED, 1L, TRANSLOG_UUID, null);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        Store store = new Store(
            shardId,
            indexSettings,
            new NIOFSDirectory(indexDir),
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath
        );
        store.createEmpty(org.apache.lucene.util.Version.LATEST);
        LuceneCommitter committer = new LuceneCommitter(new CommitterConfig(buildEngineConfig(indexSettings, store, shardId, translogDir)));
        Path parquetDir = dataPath.resolve(PARQUET_FORMAT);
        Files.createDirectories(parquetDir);
        return new TestEnv(committer, store, shardPath, indexDir, parquetDir, translogDir);
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
            Map<String, Collection<String>> failed = new HashMap<>();
            for (Map.Entry<String, Collection<String>> entry : filesToDelete.entrySet()) {
                Collection<String> failedFiles = new ArrayList<>();
                for (String file : entry.getValue()) {
                    if (Files.deleteIfExists(dir.resolve(file)) == false) {
                        failedFiles.add(file);
                    }
                }
                if (!failedFiles.isEmpty()) {
                    failed.put(entry.getKey(), failedFiles);
                }
            }
            return failed;
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
            snapshot.setUserData(commitUserData(maxSeqNo, localCP), true);
            Map<String, String> cd = new HashMap<>(snapshot.getUserData());
            cd.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());
            cd.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(snapshot.getId()));
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
        cd.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(initial.getId()));
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
            Segment seg0 = buildSegment(0, lucene0, env.indexDir, parquet0, env.parquetDir);
            CatalogSnapshotManager manager = bootstrap(env, List.of(seg0), 100, 100, policy);

            // --- CS2: ingest, refresh, flush (lcp=200, globalCP=100) ---
            // Each snapshot includes ALL accumulated segments (matching production behavior)
            Set<String> lucene1 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet1 = ingestParquetFiles(env.parquetDir, "_1.parquet");
            Segment seg1 = buildSegment(1, lucene1, env.indexDir, parquet1, env.parquetDir);
            manager.commitNewSnapshot(List.of(seg0, seg1));
            doFlush(manager, env.committer, 200, 200);

            // --- CS3: ingest, refresh, flush (lcp=300, globalCP=100) ---
            Set<String> lucene2 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet2 = ingestParquetFiles(env.parquetDir, "_2.parquet");
            Segment seg2 = buildSegment(2, lucene2, env.indexDir, parquet2, env.parquetDir);
            manager.commitNewSnapshot(List.of(seg0, seg1, seg2));
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

            // --- Advance globalCP to 200, refresh + flush to trigger policy ---
            globalCP.set(200);
            manager.commitNewSnapshot(List.of(seg0, seg1, seg2));
            doFlush(manager, env.committer, 300, 300);

            // CS1 deleted by policy. CS2=safe(200<=200), CS3, CS4(flush)=last → 3 commits
            assertEquals("CS2 + CS3 + CS4", 3, countLuceneCommits(env.store));

            // All segment files still alive — CS2 and later reference all segments
            for (String f : parquet0)
                assertTrue("parquet0 alive (ref'd by CS2): " + f, fileExists(env.parquetDir, f));
            for (String f : lucene0)
                assertTrue("lucene0 alive (ref'd by CS2): " + f, fileExists(env.indexDir, f));
            for (String f : parquet1)
                assertTrue("parquet1 alive (safe): " + f, fileExists(env.parquetDir, f));
            for (String f : lucene1)
                assertTrue("lucene1 alive (safe): " + f, fileExists(env.indexDir, f));
            for (String f : parquet2)
                assertTrue("parquet2 alive: " + f, fileExists(env.parquetDir, f));
            for (String f : lucene2)
                assertTrue("lucene2 alive: " + f, fileExists(env.indexDir, f));

            // --- Advance globalCP to 300, refresh + flush: all old commits deleted ---
            globalCP.set(300);
            manager.commitNewSnapshot(List.of(seg0, seg1, seg2));
            doFlush(manager, env.committer, 300, 300);

            // CS2, CS3, CS4 all deleted. Only CS6(flush)=safe=last remains
            assertEquals("Only latest commit remains", 1, countLuceneCommits(env.store));

            // All segment files still alive — the latest commit references all segments
            for (String f : parquet0)
                assertTrue("parquet0 alive (ref'd by latest): " + f, fileExists(env.parquetDir, f));
            for (String f : lucene0)
                assertTrue("lucene0 alive (ref'd by latest): " + f, fileExists(env.indexDir, f));
            for (String f : parquet1)
                assertTrue("parquet1 alive (ref'd by latest): " + f, fileExists(env.parquetDir, f));
            for (String f : lucene1)
                assertTrue("lucene1 alive (ref'd by latest): " + f, fileExists(env.indexDir, f));
            for (String f : parquet2)
                assertTrue("parquet2 alive (ref'd by latest): " + f, fileExists(env.parquetDir, f));
            for (String f : lucene2)
                assertTrue("lucene2 alive (ref'd by latest): " + f, fileExists(env.indexDir, f));

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
            manager.commitNewSnapshot(
                List.of(
                    buildSegment(0, lucene0, env.indexDir, parquet0, env.parquetDir),
                    buildSegment(1, lucene1, env.indexDir, parquet1, env.parquetDir),
                    buildSegment(2, lucene2, env.indexDir, parquet2, env.parquetDir)
                )
            );
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
            Segment seg0 = buildSegment(0, lucene0, env.indexDir, parquet0, env.parquetDir);
            CatalogSnapshotManager manager = bootstrap(env, List.of(seg0), 100, 100, policy);

            // Hold CS1
            GatedCloseable<CatalogSnapshot> held = manager.acquireCommittedSnapshot(false);

            // Refresh: CS2 includes all accumulated segments
            Set<String> lucene1 = ingestLuceneDocs(env.committer, env.store);
            Set<String> parquet1 = ingestParquetFiles(env.parquetDir, "_1.parquet");
            Segment seg1 = buildSegment(1, lucene1, env.indexDir, parquet1, env.parquetDir);
            manager.commitNewSnapshot(List.of(seg0, seg1));

            globalCP.set(200);
            doFlush(manager, env.committer, 200, 200);

            // CS1 protected by held snapshot — all files alive
            for (String f : lucene0)
                assertTrue("protected: " + f, fileExists(env.indexDir, f));
            for (String f : parquet0)
                assertTrue("protected: " + f, fileExists(env.parquetDir, f));
            assertTrue("CS1's commit still exists", countLuceneCommits(env.store) >= 2);

            // Release — CS1 deleted, but seg0 files still alive via CS2
            held.close();

            // seg0 lucene files still alive (CS2 references them), but CS1's commit is gone
            for (String f : lucene0)
                assertTrue("alive via CS2: " + f, fileExists(env.indexDir, f));
            // seg0 parquet files still alive (CS2 references them)
            for (String f : parquet0)
                assertTrue("alive via CS2: " + f, fileExists(env.parquetDir, f));
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
        Path translogDir = dataPath.resolve("translog");
        Files.createDirectories(translogDir);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        Set<String> lucene0, lucene1, lucene2;

        // Phase 1: Pre-crash — 3 commits
        {
            Translog.createEmptyTranslog(translogDir, shardId, SequenceNumbers.NO_OPS_PERFORMED, 1L, TRANSLOG_UUID, null);
            Store store = new Store(
                shardId,
                indexSettings,
                new NIOFSDirectory(indexDir),
                new DummyShardLock(shardId),
                Store.OnClose.EMPTY,
                shardPath
            );
            store.createEmpty(org.apache.lucene.util.Version.LATEST);
            LuceneCommitter committer = new LuceneCommitter(
                new CommitterConfig(buildEngineConfig(indexSettings, store, shardId, translogDir))
            );

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
            cd1.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(cs1.getId()));
            committer.commit(cd1);

            lucene1 = ingestLuceneDocs(committer, store);
            Set<String> parquet1 = ingestParquetFiles(parquetDir, "_1.parquet");
            CatalogSnapshot cs2 = CatalogSnapshotManager.createInitialSnapshot(
                2L,
                2L,
                0L,
                List.of(buildSegment(0, lucene0, indexDir, parquet0, parquetDir), buildSegment(1, lucene1, indexDir, parquet1, parquetDir)),
                2L,
                commitUserData(200, 200)
            );
            Map<String, String> cd2 = new HashMap<>(cs2.getUserData());
            cd2.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs2.serializeToString());
            cd2.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(cs2.getId()));
            committer.commit(cd2);

            lucene2 = ingestLuceneDocs(committer, store);
            Set<String> parquet2 = ingestParquetFiles(parquetDir, "_2.parquet");
            CatalogSnapshot cs3 = CatalogSnapshotManager.createInitialSnapshot(
                3L,
                3L,
                0L,
                List.of(
                    buildSegment(0, lucene0, indexDir, parquet0, parquetDir),
                    buildSegment(1, lucene1, indexDir, parquet1, parquetDir),
                    buildSegment(2, lucene2, indexDir, parquet2, parquetDir)
                ),
                3L,
                commitUserData(300, 300)
            );
            Map<String, String> cd3 = new HashMap<>(cs3.getUserData());
            cd3.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs3.serializeToString());
            cd3.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(cs3.getId()));
            committer.commit(cd3);

            assertEquals(3, DirectoryReader.listCommits(store.directory()).size());
            committer.close();
            store.close();
        }

        // Phase 2: Recovery — globalCP=100
        {
            // Recreate translog with globalCP=100 to simulate persisted state at crash
            org.opensearch.common.util.io.IOUtils.rm(translogDir);
            Files.createDirectories(translogDir);
            Translog.createEmptyTranslog(translogDir, shardId, 100L, 1L, TRANSLOG_UUID, null);

            var policy = new CombinedCatalogSnapshotDeletionPolicy(logger, new DefaultTranslogDeletionPolicy(-1, -1, 0), () -> 100L);
            Store store = new Store(
                shardId,
                indexSettings,
                new NIOFSDirectory(indexDir),
                new DummyShardLock(shardId),
                Store.OnClose.EMPTY,
                shardPath
            );
            LuceneCommitter committer = new LuceneCommitter(
                new CommitterConfig(buildEngineConfig(indexSettings, store, shardId, translogDir))
            );

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
        Path translogDir = dataPath.resolve("translog");
        Files.createDirectories(translogDir);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        Set<String> lucene0;

        // Phase 1: Pre-crash — 2 commits
        {
            Translog.createEmptyTranslog(translogDir, shardId, SequenceNumbers.NO_OPS_PERFORMED, 1L, TRANSLOG_UUID, null);
            Store store = new Store(
                shardId,
                indexSettings,
                new NIOFSDirectory(indexDir),
                new DummyShardLock(shardId),
                Store.OnClose.EMPTY,
                shardPath
            );
            store.createEmpty(org.apache.lucene.util.Version.LATEST);
            LuceneCommitter committer = new LuceneCommitter(
                new CommitterConfig(buildEngineConfig(indexSettings, store, shardId, translogDir))
            );

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
            cd1.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(cs1.getId()));
            committer.commit(cd1);

            Set<String> lucene1 = ingestLuceneDocs(committer, store);
            Set<String> parquet1 = ingestParquetFiles(parquetDir, "_1.parquet");
            CatalogSnapshot cs2 = CatalogSnapshotManager.createInitialSnapshot(
                2L,
                2L,
                0L,
                List.of(buildSegment(0, lucene0, indexDir, parquet0, parquetDir), buildSegment(1, lucene1, indexDir, parquet1, parquetDir)),
                2L,
                commitUserData(200, 200)
            );
            Map<String, String> cd2 = new HashMap<>(cs2.getUserData());
            cd2.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, cs2.serializeToString());
            cd2.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(cs2.getId()));
            committer.commit(cd2);

            committer.close();
            store.close();
        }

        // Phase 2: Recovery + normal operation
        {
            // Recreate translog with globalCP=100
            org.opensearch.common.util.io.IOUtils.rm(translogDir);
            Files.createDirectories(translogDir);
            Translog.createEmptyTranslog(translogDir, shardId, 100L, 1L, TRANSLOG_UUID, null);

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
            LuceneCommitter committer = new LuceneCommitter(
                new CommitterConfig(buildEngineConfig(indexSettings, store, shardId, translogDir))
            );

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
            manager.commitNewSnapshot(
                List.of(
                    buildSegment(0, lucene0, indexDir, Set.of("_0.parquet"), parquetDir),
                    buildSegment(3, lucene3, indexDir, parquet3, parquetDir)
                )
            );

            globalCP.set(300);
            doFlush(manager, committer, 300, 300);

            assertEquals("Only CS3's commit remains", 1, DirectoryReader.listCommits(store.directory()).size());
            // lucene0 files still alive — CS3 references seg0
            for (String f : lucene0)
                assertTrue("lucene0 alive via CS3: " + f, fileExists(indexDir, f));
            assertTrue("_0.parquet alive via CS3", fileExists(parquetDir, "_0.parquet"));
            for (String f : lucene3)
                assertTrue(fileExists(indexDir, f));
            assertTrue(fileExists(parquetDir, "_3.parquet"));

            manager.close();
            committer.close();
            store.close();
        }
    }
}
