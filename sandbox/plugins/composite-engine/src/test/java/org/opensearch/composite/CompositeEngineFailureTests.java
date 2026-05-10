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
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.stub.FileBackedFailableIndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.stub.FileBackedFailableWriter;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
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

        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        bootstrapStoreWithMetadata(s, uuid);

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
        EngineConfig config = new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(engineSettings)
            .store(s)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .translogConfig(tc)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(List.of())
            .internalRefreshListener(List.of())
            .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(primaryTerm::get)
            .tombstoneDocSupplier(tombstoneDocSupplier())
            .dataFormatRegistry(dfaRegistry)
            .committerFactory(c -> new InMemoryCommitter(s))
            .eventListener(new Engine.EventListener() {
                @Override
                public void onFailedEngine(String reason, Exception e) {}
            })
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

    public void testAbortedWriterClosedNoFileOnDisk() throws IOException {
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
            fbPrimary.getLastCreatedWriter().rollbackFailure = new IOException("rollback failed");

            try {
                engine.index(indexOp(doc("5")));
            } catch (UnsupportedOperationException e) { /* expected */ }

            assertTrue("inner writer closed", innerWriter.isClosed());
            assertFalse("no file on disk (not flushed)", java.nio.file.Files.exists(innerWriter.getFilePath()));

            assertThat(engine.index(indexOp(doc("6"))).getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            assertNotSame(innerWriter, fbPrimary.getLastCreatedWriter());
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
}
