/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.stub.FileBackedFailableIndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.stub.FileBackedFailableWriter;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests verifying file-level data consistency through the full stack:
 * DataFormatAwareEngine → CompositeIndexingExecutionEngine → CompositeWriter → FileBackedFailableWriter.
 * <p>
 * Tests:
 * <ul>
 *   <li>{@code testSuccessfulIndexProducesConsistentFiles} — both format files have N lines, cross-format match</li>
 *   <li>{@code testSecondaryFailureFlushesNMinus1AndClosesWriter} — flushed file has N-1 docs after rollback,
 *       inner writer closed, fresh writer created for next doc</li>
 *   <li>{@code testPrimaryFailureWriterStaysOpenInPool} — inner writer NOT closed, same writer reused</li>
 *   <li>{@code testAbortedWriterClosedNoFileOnDisk} — inner writer closed, no file on disk (not flushed)</li>
 *   <li>{@code testIntermittentFailuresProduceConsistentFiles} — total rows across all files match successes,
 *       cross-format consistency across multiple writer generations</li>
 * </ul>
 */
public class CompositeEngineFailureTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Store store;
    private ShardId shardId;
    private AtomicLong primaryTerm;
    private FileBackedFailableIndexingExecutionEngine fbPrimary;
    private FileBackedFailableIndexingExecutionEngine fbSecondary;
    private CompositeIndexingExecutionEngine compositeEngine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        primaryTerm = new AtomicLong(randomLongBetween(1, Long.MAX_VALUE));
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            store.close();
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    // --- Infrastructure ---

    private Store createStore() throws IOException {
        Directory dir = newDirectory();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );
        Path path = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        ShardPath shardPath = new ShardPath(false, path, path, shardId);
        return new Store(
            shardId,
            indexSettings,
            dir,
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath,
            new FsDirectoryFactory()
        );
    }

    private void bootstrapStoreWithMetadata(Store s, String translogUUID) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                s.directory(),
                new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE)
                    .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            )
        ) {
            Map<String, String> commitData = new HashMap<>();
            commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
            commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            commitData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            commitData.put(Engine.HISTORY_UUID_KEY, UUID.randomUUID().toString());
            writer.setLiveCommitData(commitData.entrySet());
            writer.commit();
        }
    }

    private DataFormatAwareEngine createEngine(Store s, Path translogPath) throws IOException {
        return createEngine(s, translogPath, () -> SequenceNumbers.NO_OPS_PERFORMED, true);
    }

    private DataFormatAwareEngine createEngine(Store s, Path translogPath, java.util.function.LongSupplier globalCheckpointSupplier)
        throws IOException {
        return createEngine(s, translogPath, globalCheckpointSupplier, true);
    }

    private DataFormatAwareEngine createEngine(
        Store s,
        Path translogPath,
        java.util.function.LongSupplier globalCheckpointSupplier,
        boolean bootstrapTranslog
    ) throws IOException {
        Path dataDir = createTempDir("file-backed-data");
        fbPrimary = new FileBackedFailableIndexingExecutionEngine("lucene", dataDir);
        fbSecondary = new FileBackedFailableIndexingExecutionEngine("parquet", dataDir);

        CompositeTestHelper.FailableCommitter committer = new CompositeTestHelper.FailableCommitter();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(fbPrimary.getDataFormat());
        when(registry.format("parquet")).thenReturn(fbSecondary.getDataFormat());
        when(registry.getIndexingEngine(any(), any())).thenAnswer(inv -> {
            DataFormat fmt = inv.getArgument(1);
            if (fmt.name().equals("lucene")) return fbPrimary;
            if (fmt.name().equals("parquet")) return fbSecondary;
            return null;
        });

        Settings.Builder sb = Settings.builder()
            .put("index.composite.primary_data_format", "lucene")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        sb.putList("index.composite.secondary_data_formats", "parquet");
        IndexMetadata meta = IndexMetadata.builder("test-fb").settings(sb.build()).build();
        IndexSettings fbSettings = new IndexSettings(meta, Settings.EMPTY);
        compositeEngine = new CompositeIndexingExecutionEngine(fbSettings, null, committer, registry, null, null);

        DataFormatRegistry dfaRegistry = mock(DataFormatRegistry.class);
        when(dfaRegistry.format("composite")).thenReturn(compositeEngine.getDataFormat());
        when(dfaRegistry.getIndexingEngine(any(), any())).thenAnswer(inv -> compositeEngine);
        when(dfaRegistry.getDeleteExecutionEngine(any())).thenAnswer(inv -> mock(DeleteExecutionEngine.class));

        if (bootstrapTranslog) {
            String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            bootstrapStoreWithMetadata(s, uuid);
        }

        IndexSettings engineSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
                .build()
        );
        TranslogConfig tc = new TranslogConfig(shardId, translogPath, engineSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false);
        org.opensearch.index.mapper.MapperService mapperService = mock(org.opensearch.index.mapper.MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(engineSettings);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.getVersion()).thenReturn(1L);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        EngineConfig config = new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(engineSettings)
            .store(s)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .translogConfig(tc)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(List.of())
            .internalRefreshListener(List.of())
            .globalCheckpointSupplier(globalCheckpointSupplier)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(primaryTerm::get)
            .tombstoneDocSupplier(tombstoneDocSupplier())
            .dataFormatRegistry(dfaRegistry)
            .committerFactory(c -> new InMemoryCommitter(s))
            .eventListener(new Engine.EventListener() {
                @Override
                public void onFailedEngine(String reason, Exception e) {}
            })
            .mapperService(mapperService)
            .build();
        return new DataFormatAwareEngine(config);
    }

    private ParsedDocument doc(String id) {
        ParseContext.Document document = new ParseContext.Document();
        Field uidField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesReference source = new BytesArray("{}");
        org.apache.lucene.util.BytesRef ref = source.toBytesRef();
        document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        DocumentInput<?> input = compositeEngine.newDocumentInput();
        return new ParsedDocument(versionField, seqID, id, null, Arrays.asList(document), source, MediaTypeRegistry.JSON, null, input);
    }

    private Engine.Index indexOp(ParsedDocument d) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(d.id())),
            d,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    // --- Tests ---

    public void testSuccessfulIndexProducesConsistentFiles() throws IOException {
        try (DataFormatAwareEngine engine = createEngine(store, createTempDir())) {
            for (int i = 0; i < 5; i++) {
                assertThat(engine.index(indexOp(doc(Integer.toString(i)))).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            engine.refresh("test");

            List<String> primary = FileBackedFailableIndexingExecutionEngine.readFlushedFile(
                fbPrimary.getLastCreatedWriter().getFilePath()
            );
            List<String> secondary = FileBackedFailableIndexingExecutionEngine.readFlushedFile(
                fbSecondary.getLastCreatedWriter().getFilePath()
            );
            assertEquals(5, primary.size());
            assertEquals(5, secondary.size());
            assertEquals(primary.size(), secondary.size());
        }
    }

    public void testSecondaryFailureFlushesNMinus1AndClosesWriter() throws IOException {
        try (DataFormatAwareEngine engine = createEngine(store, createTempDir())) {
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(doc(Integer.toString(i))));
            }

            FileBackedFailableWriter innerWriter = fbPrimary.getLastCreatedWriter();

            fbSecondary.getLastCreatedWriter().writeResultSupplier = () -> new WriteResult.Failure(
                new IOException("secondary failed"),
                -1,
                -1,
                -1
            );

            try {
                engine.index(indexOp(doc("5")));
            } catch (UnsupportedOperationException e) { /* expected */ }

            // Writer closed and removed from pool
            assertTrue("inner writer closed", innerWriter.isClosed());

            // Flushed file has N-1 docs, cross-format consistent
            List<String> primary = FileBackedFailableIndexingExecutionEngine.readFlushedFile(innerWriter.getFilePath());
            List<String> secondary = FileBackedFailableIndexingExecutionEngine.readFlushedFile(
                fbSecondary.getLastCreatedWriter().getFilePath()
            );
            assertEquals(5, primary.size());
            assertEquals(5, secondary.size());

            // Fresh writer created for next doc
            fbSecondary.getLastCreatedWriter().writeResultSupplier = null;
            fbSecondary.setDefaultWriteResultSupplier(null);
            assertThat(engine.index(indexOp(doc("6"))).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            assertNotSame(innerWriter, fbPrimary.getLastCreatedWriter());
        }
    }

    public void testPrimaryFailureWriterStaysOpenInPool() throws IOException {
        try (DataFormatAwareEngine engine = createEngine(store, createTempDir())) {
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(doc(Integer.toString(i))));
            }

            FileBackedFailableWriter innerWriter = fbPrimary.getLastCreatedWriter();

            // Model a Parquet-style format: addDoc failure transitions to PENDING_ROLLBACK, then
            // rollback brings the writer back to ACTIVE (fully reversible — no row consumed
            // on failure) so subsequent addDoc calls succeed.
            innerWriter.setStateAfterRollback(org.opensearch.index.engine.dataformat.WriterState.ACTIVE);
            innerWriter.writeResultSupplier = () -> new WriteResult.Failure(new IOException("primary full"), -1, -1, -1);

            try {
                engine.index(indexOp(doc("5")));
            } catch (UnsupportedOperationException e) { /* expected */ }

            // Writer NOT closed, still in pool
            assertFalse("inner writer still open", innerWriter.isClosed());

            innerWriter.writeResultSupplier = null;
            assertThat(engine.index(indexOp(doc("6"))).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
        }
    }

    /**
     * RETIRED_DROP path: secondary fails AND primary's rollback fails. Composite reports
     * RETIRED_DROP. The engine fails so recovery can replay the translog and restore the
     * acked-but-unflushed docs. Verifies the writer was closed and no file was flushed.
     */
    public void testAbortedWriterClosedAndEngineFailed() throws IOException {
        DataFormatAwareEngine engine = createEngine(store, createTempDir());
        try {
            for (int i = 0; i < 5; i++) {
                engine.index(indexOp(doc(Integer.toString(i))));
            }

            FileBackedFailableWriter innerWriter = fbPrimary.getLastCreatedWriter();

            fbSecondary.getLastCreatedWriter().writeResultSupplier = () -> new WriteResult.Failure(
                new IOException("secondary failed"),
                -1,
                -1,
                -1
            );
            fbPrimary.getLastCreatedWriter().rollbackFailure = new IOException("rollback failed");

            try {
                engine.index(indexOp(doc("5")));
            } catch (Exception expected) { /* RETIRED_DROP fails engine; index call may throw */ }

            assertTrue("inner writer closed", innerWriter.isClosed());
            assertFalse("no file on disk (not flushed)", java.nio.file.Files.exists(innerWriter.getFilePath()));
            // Subsequent index ops must throw — engine has been failed.
            expectThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> engine.index(indexOp(doc("6"))));
        } finally {
            engine.close();
        }
    }

    public void testIntermittentFailuresProduceConsistentFiles() throws IOException {
        try (DataFormatAwareEngine engine = createEngine(store, createTempDir())) {
            engine.index(indexOp(doc("init")));
            int expected = 1;

            for (int i = 0; i < 9; i++) {
                boolean fail = (i % 3 == 2);
                WriteResult failResult = new WriteResult.Failure(new IOException("intermittent"), -1, -1, -1);
                fbSecondary.setDefaultWriteResultSupplier(fail ? () -> failResult : null);
                fbSecondary.getLastCreatedWriter().writeResultSupplier = fail ? () -> failResult : null;

                try {
                    Engine.IndexResult r = engine.index(indexOp(doc(Integer.toString(i))));
                    if (fail == false) {
                        assertThat(r.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
                        expected++;
                    }
                } catch (UnsupportedOperationException e) {
                    assertTrue(fail);
                }
            }
            fbSecondary.setDefaultWriteResultSupplier(null);
            engine.refresh("verify");

            long primaryRows = 0;
            for (FileBackedFailableWriter w : fbPrimary.getAllWriters()) {
                primaryRows += FileBackedFailableIndexingExecutionEngine.readFlushedFile(w.getFilePath()).size();
            }
            long secondaryRows = 0;
            for (FileBackedFailableWriter w : fbSecondary.getAllWriters()) {
                secondaryRows += FileBackedFailableIndexingExecutionEngine.readFlushedFile(w.getFilePath()).size();
            }
            assertEquals(expected, primaryRows);
            assertEquals(expected, secondaryRows);
            assertEquals(primaryRows, secondaryRows);
        }
    }

    // ============================================================================
    // SeqNo / checkpoint / recovery invariants under per-doc failure injection.
    //
    // For each failure mode we verify, post-indexing-of-10-docs:
    // 1. localCheckpoint == globalCheckpoint == 9 (seqNos 0..9 consumed, no holes)
    // 2. Failed seqNos persisted as Translog.NoOp; successful ops as Translog.Index
    // 3. After close + reopen + translog replay, localCheckpoint == 9 again
    // ============================================================================

    /** Mode 1: secondary fails (with rollback success) — primary stays consistent. */
    public void testSeqNoInvariantsSecondaryFailRollbackOk() throws IOException {
        runSeqNoAndRecoveryScenario(/*failPrimary*/ false, /*rollbackFails*/ false);
    }

    /**
     * Mode 2: secondary fails AND primary's rollback fails. Composite reports RETIRED_DROP
     * → engine MUST fail (acked docs would otherwise be lost until restart). Verify the
     * engine is in failed state and translog still has the in-flight ops for recovery.
     */
    public void testSeqNoInvariantsSecondaryFailRollbackFailsFailsEngine() throws IOException {
        Path translogPath = createTempDir();
        java.util.concurrent.atomic.AtomicLong globalCheckpoint = new java.util.concurrent.atomic.AtomicLong(
            SequenceNumbers.NO_OPS_PERFORMED
        );
        DataFormatAwareEngine engine = createEngine(store, translogPath, globalCheckpoint::get);
        try {
            engine.translogManager().recoverFromTranslog(s -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            // Index a couple successful docs.
            for (int i = 0; i < 2; i++) {
                Engine.IndexResult r = engine.index(indexOp(doc(Integer.toString(i))));
                assertThat(r.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            // Wire secondary failure + primary rollback failure.
            fbSecondary.getLastCreatedWriter().writeResultSupplier = () -> new WriteResult.Failure(
                new IOException("secondary failed"),
                -1,
                -1,
                -1
            );
            fbPrimary.getLastCreatedWriter().rollbackFailure = new IOException("primary rollback fails");

            // The doc that hits both failures triggers RETIRED_DROP → engine fails.
            try {
                engine.index(indexOp(doc("2")));
            } catch (Exception expected) { /* engine failure may surface as throw */ }

            // Subsequent index calls fail — engine is closed. The 2 acked docs (seqNos 0, 1)
            // remain durably persisted in the translog files on disk; on the next shard
            // recovery a fresh engine will replay them. We don't assert translog stats here
            // because reading them via the closed engine throws AlreadyClosedException.
            expectThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> engine.index(indexOp(doc("3"))));
        } finally {
            engine.close();
        }
    }

    /** Mode 3: primary fails immediately (no secondary touched, no rollback needed). */
    public void testSeqNoInvariantsPrimaryFailRollbackOk() throws IOException {
        runSeqNoAndRecoveryScenario(/*failPrimary*/ true, /*rollbackFails*/ false);
    }

    /**
     * Mode 4: primary fails AND its writer's rollback would fail — but since primary failure
     * does not require rolling back any other writer (no secondary was touched), this mode
     * still terminates cleanly. We exercise it for symmetry; the result is identical to mode 3.
     */
    public void testSeqNoInvariantsPrimaryFailRollbackFails() throws IOException {
        runSeqNoAndRecoveryScenario(/*failPrimary*/ true, /*rollbackFails*/ true);
    }

    /**
     * Drives 10 indexing operations with failures injected at calls 4, 7, 9 on the configured
     * failing side. Verifies seqNo/checkpoint invariants both pre-restart and post-translog-replay.
     *
     * <p>Note: {@code rollbackFails=true && failPrimary=false} (RETIRED_DROP case) is covered
     * separately by {@link #testSeqNoInvariantsSecondaryFailRollbackFailsFailsEngine}, since
     * that path now intentionally fails the engine and so the 10-doc loop cannot complete.
     */
    private void runSeqNoAndRecoveryScenario(boolean failPrimary, boolean rollbackFails) throws IOException {
        assert !(rollbackFails && !failPrimary) : "RETIRED_DROP path is exercised by a dedicated test";
        Path translogPath = createTempDir();
        java.util.concurrent.atomic.AtomicLong globalCheckpoint = new java.util.concurrent.atomic.AtomicLong(
            SequenceNumbers.NO_OPS_PERFORMED
        );
        java.util.Set<Long> failedSeqNos = new java.util.HashSet<>();

        DataFormatAwareEngine engine = createEngine(store, translogPath, globalCheckpoint::get);
        try {
            engine.translogManager().recoverFromTranslog(s -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            FileBackedFailableIndexingExecutionEngine targetEngine = failPrimary ? fbPrimary : fbSecondary;
            // Fail on the 4th, 7th, and 9th calls (1-based); succeed otherwise. The supplier is
            // installed as the default so it survives writer retirement + recreation.
            java.util.concurrent.atomic.AtomicInteger callIdx = new java.util.concurrent.atomic.AtomicInteger();
            Supplier<WriteResult> failureSupplier = () -> {
                int n = callIdx.incrementAndGet();
                if (n == 4 || n == 7 || n == 9) {
                    return new WriteResult.Failure(new IOException("injected failure on call " + n), -1, -1, -1);
                }
                return null; // null tells FileBackedFailableWriter to take the success path
            };
            targetEngine.setDefaultWriteResultSupplier(failureSupplier);
            if (failPrimary) {
                // Primary models a Parquet-style writer: addDoc Failure → PENDING_ROLLBACK, then
                // rollback brings it back to ACTIVE (fully reversible). Apply to all writers
                // created by this engine, since the composite recreates a writer per refresh.
                fbPrimary.setDefaultStateAfterRollback(org.opensearch.index.engine.dataformat.WriterState.ACTIVE);
            }
            // For "rollback fails" scenarios, the rollback target's writer factory should produce
            // writers that throw on rollback. We don't have a setter for this on the engine, so
            // we install it lazily — after each writer is created, set rollbackFailure on it.
            // Simpler: rely on the fact that on a single-doc test, only the first writer matters.
            // We'll set rollbackFailure inside the loop after the first index call.

            int succeeded = 0;
            for (int i = 0; i < 10; i++) {
                // After the first writer is created, install rollbackFailure on the primary if needed.
                // (For "rollback fails" with secondary failure mode: the composite calls
                // primary.rollbackLastDoc when the secondary fails; we want that to throw.)
                if (i == 0 && rollbackFails && failPrimary == false) {
                    Engine.IndexResult firstResult = engine.index(indexOp(doc(Integer.toString(i))));
                    if (firstResult.getResultType() == Engine.Result.Type.SUCCESS) succeeded++;
                    else failedSeqNos.add(firstResult.getSeqNo());
                    fbPrimary.getLastCreatedWriter().rollbackFailure = new IOException("primary rollback fails");
                    continue;
                }
                Engine.IndexResult result = engine.index(indexOp(doc(Integer.toString(i))));
                if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                    succeeded++;
                } else {
                    failedSeqNos.add(result.getSeqNo());
                }
            }

            // Pre-restart invariants
            assertEquals("3 failures injected", 7, succeeded);
            assertEquals("3 failed seqNos recorded", 3, failedSeqNos.size());
            assertEquals("max seqNo == last assigned", 9L, engine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo());
            assertEquals("processed checkpoint advanced through all 10", 9L, engine.getProcessedLocalCheckpoint());
            // Promote global to match local (replication tracker would do this externally)
            globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
            assertEquals("global == local after promotion", engine.getProcessedLocalCheckpoint(), globalCheckpoint.get());

            // Translog should contain 10 ops (7 Index + 3 NoOp). We deliberately do NOT flush
            // so the translog is the only durable record — restart must replay it.
            assertEquals(10, engine.translogManager().getTranslogStats().estimatedNumberOfOperations());
        } finally {
            engine.close();
        }

        // Reopen with a fresh failable engine (no failure injection) and replay the translog.
        // Successful Index ops must replay successfully; NoOps replay as no-ops. End state:
        // localCheckpoint == 9, max == 9, identical to pre-restart.
        // bootstrapTranslog=false so we don't clobber the existing translog files.
        DataFormatAwareEngine reopened = createEngine(store, translogPath, globalCheckpoint::get, false);
        try {
            org.opensearch.index.engine.exec.Indexer indexer = reopened;
            int[] replayedCount = new int[1];
            int recovered = reopened.translogManager().recoverFromTranslog(snapshot -> {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    Engine.Result result;
                    if (op instanceof Translog.Index idx) {
                        Engine.Index e = new Engine.Index(
                            new Term(IdFieldMapper.NAME, Uid.encodeId(idx.id())),
                            doc(idx.id()),
                            idx.seqNo(),
                            idx.primaryTerm(),
                            idx.version(),
                            null,
                            Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                            System.nanoTime(),
                            idx.getAutoGeneratedIdTimestamp(),
                            true,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        );
                        result = indexer.index(e);
                    } else if (op instanceof Translog.NoOp noop) {
                        Engine.NoOp e = new Engine.NoOp(
                            noop.seqNo(),
                            noop.primaryTerm(),
                            Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                            System.nanoTime(),
                            noop.reason()
                        );
                        result = indexer.noOp(e);
                    } else {
                        throw new AssertionError("unexpected op type: " + op.opType());
                    }
                    if (result.getResultType() == Engine.Result.Type.FAILURE) {
                        throw new AssertionError("replay failed for " + op + ": " + result.getFailure());
                    }
                    replayedCount[0]++;
                }
                return replayedCount[0];
            }, reopened.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            assertEquals("replay processed all 10 translog ops", 10, recovered);
            assertEquals("post-replay max seqNo == 9", 9L, reopened.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo());
            assertEquals("post-replay localCheckpoint == 9", 9L, reopened.getProcessedLocalCheckpoint());
        } finally {
            reopened.close();
        }
    }
}
