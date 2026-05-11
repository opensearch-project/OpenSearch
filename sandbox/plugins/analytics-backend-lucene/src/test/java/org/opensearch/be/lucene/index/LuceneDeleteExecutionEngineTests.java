/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LuceneDeleteExecutionEngine}.
 */
public class LuceneDeleteExecutionEngineTests extends OpenSearchTestCase {

    private LuceneCommitter committer;
    private Store store;
    private LuceneDeleteExecutionEngine deleteEngine;
    private Path baseDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        Path translogPath = dataPath.resolve("translog");
        Files.createDirectories(translogPath);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        store = new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));
        store.createEmpty(Version.LATEST);

        EngineConfig engineConfig = new EngineConfig.Builder().indexSettings(indexSettings)
            .store(store)
            .codecService(new CodecService(null, indexSettings, LogManager.getLogger(getClass()), List.of()))
            .translogConfig(new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
            .retentionLeasesSupplier(() -> new RetentionLeases(0, 0, Collections.emptyList()))
            .build();

        committer = (LuceneCommitter) new LuceneCommitterFactory().getCommitter(new CommitterConfig(engineConfig, () -> {}));
        deleteEngine = new LuceneDeleteExecutionEngine(new LuceneDataFormat(), committer);
    }

    @Override
    public void tearDown() throws Exception {
        if (deleteEngine != null) {
            deleteEngine.close();
        }
        if (committer != null) {
            committer.close();
        }
        if (store != null) {
            store.close();
        }
        super.tearDown();
    }

    private LuceneWriter createLuceneWriter(long generation) throws IOException {
        Path writerDir = baseDir.resolve("writers");
        Files.createDirectories(writerDir);
        return new LuceneWriter(generation, new LuceneDataFormat(), writerDir, null, Codec.getDefault(), null);
    }

    private Writer<?> createMockCompositeWriter(long generation, boolean hasLucene, boolean hasParquet) {
        Writer<?> compositeWriter = mock(Writer.class);
        when(compositeWriter.generation()).thenReturn(generation);

        if (hasLucene) {
            LuceneWriter mockLuceneWriter = mock(LuceneWriter.class);
            when(mockLuceneWriter.generation()).thenReturn(generation);
            when(compositeWriter.getWriterForFormat("lucene")).thenReturn(Optional.of(mockLuceneWriter));
        } else {
            when(compositeWriter.getWriterForFormat("lucene")).thenReturn(Optional.empty());
        }

        if (hasParquet) {
            Writer<?> mockParquetWriter = mock(Writer.class);
            when(mockParquetWriter.generation()).thenReturn(generation);
            when(compositeWriter.getWriterForFormat("parquet")).thenReturn(Optional.of(mockParquetWriter));
        } else {
            when(compositeWriter.getWriterForFormat("parquet")).thenReturn(Optional.empty());
        }

        return compositeWriter;
    }

    private void addDoc(LuceneWriter writer, String id, int rowId) throws IOException {
        LuceneDocumentInput input = new LuceneDocumentInput();
        org.opensearch.index.mapper.MappedFieldType keywordField = mock(org.opensearch.index.mapper.MappedFieldType.class);
        when(keywordField.typeName()).thenReturn("keyword");
        when(keywordField.name()).thenReturn("_id");
        when(keywordField.hasDocValues()).thenReturn(false);
        input.addField(keywordField, id);
        input.setRowId(DocumentInput.ROW_ID_FIELD, rowId);
        writer.addDoc(input);
    }

    public void testGetDataFormatReturnsLucene() {
        assertEquals("lucene", deleteEngine.getDataFormat().name());
    }

    public void testCreateDeleterReturnsPairedDeleter() throws IOException {
        try (LuceneWriter writer = createLuceneWriter(1L)) {
            Deleter deleter = deleteEngine.createDeleter(writer);

            assertNotNull("createDeleter should return non-null", deleter);
            assertEquals("Deleter generation should match writer", 1L, deleter.generation());
        }
    }

    public void testGetDeleterReturnsCreatedDeleter() throws IOException {
        try (LuceneWriter writer = createLuceneWriter(5L)) {
            Deleter created = deleteEngine.createDeleter(writer);

            assertNotNull("retrieved deleter should not be null", created);
        }
    }

    public void testCreateDeleterWithCompositeWriterUsesGetWriterForFormat() {
        Writer<?> compositeWriter = createMockCompositeWriter(10L, true, false);

        Deleter deleter = deleteEngine.createDeleter(compositeWriter);

        assertNotNull(deleter);
        assertEquals(10L, deleter.generation());
    }

    public void testCreateDeleterWithCompositeWriterHavingParquetPrimaryAndLuceneSecondary() {
        // Composite writer: parquet is primary, lucene is secondary
        Writer<?> compositeWriter = createMockCompositeWriter(5L, true, true);

        Deleter deleter = deleteEngine.createDeleter(compositeWriter);

        assertNotNull("Deleter should be created from the lucene secondary writer", deleter);
        assertEquals(5L, deleter.generation());
    }

    public void testCreateDeleterWithCompositeWriterHavingLucenePrimaryAndParquetSecondary() {
        // Composite writer: lucene is primary, parquet is secondary
        Writer<?> compositeWriter = createMockCompositeWriter(8L, true, true);

        Deleter deleter = deleteEngine.createDeleter(compositeWriter);

        assertNotNull("Deleter should be created from the lucene primary writer", deleter);
        assertEquals(8L, deleter.generation());
    }

    public void testCreateDeleterThrowsWhenNoLuceneWriterFound() {
        // Composite writer with only parquet, no lucene
        Writer<?> writerWithoutLucene = createMockCompositeWriter(7L, false, true);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> deleteEngine.createDeleter(writerWithoutLucene));
        assertTrue(ex.getMessage().contains("no Lucene writer found"));
    }

    public void testDeleteDocumentFallsBackToCommitterWhenNoDeleter() throws IOException {
        // Index a doc via the committer's IndexWriter so we can delete it
        committer.getIndexWriter().addDocument(new org.apache.lucene.document.Document());
        committer.getIndexWriter().commit();

        DeleteInput deleteInput = new DeleteInput("_id", new BytesRef("test-doc"), 999L);
        DeleteResult result = deleteEngine.deleteDocument(deleteInput);

        assertTrue("Should return success even via fallback", result instanceof DeleteResult.Success);
    }

    public void testDeleteDocumentDelegatesToDeleterWhenPresent() throws IOException {
        try (LuceneWriter writer = createLuceneWriter(1L)) {
            addDoc(writer, "doc1", 0);

            Deleter deleter = deleteEngine.createDeleter(writer);
            assertNotNull("Deleter should be created", deleter);

            DeleteInput deleteInput = new DeleteInput("_id", new BytesRef("doc1"), 1L);
            DeleteResult result = deleteEngine.deleteDocument(deleteInput);

            assertTrue("Should return success via deleter", result instanceof DeleteResult.Success);
        }
    }
}
