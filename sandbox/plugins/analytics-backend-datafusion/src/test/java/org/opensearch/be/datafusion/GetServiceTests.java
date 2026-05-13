/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.index.LuceneWriterCodec;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Unit tests for {@link GetService}. The Lucene side is exercised against a real in-memory
 * index using the production {@link LuceneWriterCodec} (which stamps the {@code writer_generation}
 * segment attribute). The DataFusion native side is stubbed via the {@link GetService.NativeExecutor}
 * seam so these tests run on any platform without the native library.
 */
public class GetServiceTests extends OpenSearchTestCase {

    public void testHappyPathReturnsSourceAndForwardsRowId() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        long writerGeneration = 7L;
        writeDoc(dir, "doc-1", 0L, writerGeneration);

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            StubExecutor executor = new StubExecutor(
                Map.of("name", "alice", "age", 42L, "_seq_no", 11L, "_primary_term", 3L, "_version", 5L)
            );
            GetService service = new GetService(r -> reader, new GetService.SubstraitPlanFactory(), executor);

            CatalogSnapshot snapshot = stubSnapshot(writerGeneration, "parquet-gen-7", "/tmp/df/shard-1");
            IndexReaderProvider.Reader pr = stubReaderProvider(snapshot);

            Engine.Get get = new Engine.Get(true, false, "doc-1", new Term(IdFieldMapper.NAME, Uid.encodeId("doc-1")));
            DocumentLookupResult result = service.getById(get, pr, "my_index");

            assertTrue("expected hit", result.exists());
            assertEquals("doc-1", result.id());
            assertEquals(11L, result.seqNo());
            assertEquals(3L, result.primaryTerm());
            assertEquals(5L, result.version());
            assertNotNull("source must be present", result.source());
            String sourceJson = result.source().utf8ToString();
            assertTrue("source must carry user fields, got " + sourceJson, sourceJson.contains("\"alice\""));
            assertFalse("reserved fields must be stripped, got " + sourceJson, sourceJson.contains("_seq_no"));

            // rowId 0 must have been forwarded to the native executor.
            assertEquals(0L, executor.lastRowId);
        } finally {
            dir.close();
        }
    }

    public void testMissReturnsNotFound() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        writeDoc(dir, "doc-1", 0L, 7L);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            StubExecutor executor = new StubExecutor(Map.of());
            GetService service = new GetService(r -> reader, new GetService.SubstraitPlanFactory(), executor);

            CatalogSnapshot snapshot = stubSnapshot(7L, "parquet-gen-7", "/tmp/df/shard-1");
            IndexReaderProvider.Reader pr = stubReaderProvider(snapshot);

            Engine.Get get = new Engine.Get(true, false, "missing", new Term(IdFieldMapper.NAME, Uid.encodeId("missing")));
            DocumentLookupResult result = service.getById(get, pr, "my_index");
            assertFalse(result.exists());
            assertEquals("missing", result.id());
            assertNull(result.source());
            assertEquals("executor must not be called on a miss", -1L, executor.lastRowId);
        } finally {
            dir.close();
        }
    }

    public void testMissingWriterGenerationThrows() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        writeDocNoWriterGen(dir, "doc-1", 0L);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            GetService service = new GetService(r -> reader, new GetService.SubstraitPlanFactory(), new StubExecutor(Map.of()));
            CatalogSnapshot snapshot = stubSnapshot(7L, "parquet-gen-7", "/tmp/df/shard-1");
            IndexReaderProvider.Reader pr = stubReaderProvider(snapshot);

            Engine.Get get = new Engine.Get(true, false, "doc-1", new Term(IdFieldMapper.NAME, Uid.encodeId("doc-1")));
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> service.getById(get, pr, "my_index"));
            assertTrue("expected writer_generation complaint, got: " + ex.getMessage(), ex.getMessage().contains("writer_generation"));
        } finally {
            dir.close();
        }
    }

    public void testNoParquetFilesForGenerationThrows() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        writeDoc(dir, "doc-1", 0L, 7L);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            GetService service = new GetService(r -> reader, new GetService.SubstraitPlanFactory(), new StubExecutor(Map.of()));
            // Snapshot carries a DIFFERENT generation — no match in findParquetSet.
            CatalogSnapshot snapshot = stubSnapshot(99L, "parquet-gen-99", "/tmp/df/shard-1");
            IndexReaderProvider.Reader pr = stubReaderProvider(snapshot);

            Engine.Get get = new Engine.Get(true, false, "doc-1", new Term(IdFieldMapper.NAME, Uid.encodeId("doc-1")));
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> service.getById(get, pr, "my_index"));
            assertTrue("expected parquet file-set complaint, got: " + ex.getMessage(), ex.getMessage().contains("No parquet file-set"));
        } finally {
            dir.close();
        }
    }

    public void testExecutorNullRowReportsNotFound() throws Exception {
        Directory dir = new ByteBuffersDirectory();
        writeDoc(dir, "doc-1", 0L, 7L);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            GetService service = new GetService(r -> reader, new GetService.SubstraitPlanFactory(), new StubExecutor(null));
            CatalogSnapshot snapshot = stubSnapshot(7L, "parquet-gen-7", "/tmp/df/shard-1");
            IndexReaderProvider.Reader pr = stubReaderProvider(snapshot);

            Engine.Get get = new Engine.Get(true, false, "doc-1", new Term(IdFieldMapper.NAME, Uid.encodeId("doc-1")));
            DocumentLookupResult result = service.getById(get, pr, "my_index");
            assertFalse("parquet empty → not found", result.exists());
        } finally {
            dir.close();
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static void writeDoc(Directory dir, String id, long rowId, long writerGeneration) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(new LuceneWriterCodec(new Lucene104Codec(), writerGeneration));
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            Document doc = new Document();
            BytesRef encoded = Uid.encodeId(id);
            doc.add(new StringField(IdFieldMapper.NAME, new BytesRef(encoded.bytes, encoded.offset, encoded.length), Field.Store.NO));
            doc.add(new SortedNumericDocValuesField(DocumentInput.ROW_ID_FIELD, rowId));
            w.addDocument(doc);
            w.commit();
        }
    }

    private static void writeDocNoWriterGen(Directory dir, String id, long rowId) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig();
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            Document doc = new Document();
            BytesRef encoded = Uid.encodeId(id);
            doc.add(new StringField(IdFieldMapper.NAME, new BytesRef(encoded.bytes, encoded.offset, encoded.length), Field.Store.NO));
            doc.add(new SortedNumericDocValuesField(DocumentInput.ROW_ID_FIELD, rowId));
            w.addDocument(doc);
            w.commit();
        }
    }

    private static IndexReaderProvider.Reader stubReaderProvider(CatalogSnapshot snapshot) {
        return new IndexReaderProvider.Reader() {
            @Override
            public CatalogSnapshot catalogSnapshot() {
                return snapshot;
            }

            @Override
            public Object reader(org.opensearch.index.engine.dataformat.DataFormat format) {
                return null;
            }

            @Override
            public <R> R getReader(org.opensearch.index.engine.dataformat.DataFormat format, Class<R> readerType) {
                return null;
            }

            @Override
            public void close() {}
        };
    }

    private static CatalogSnapshot stubSnapshot(long writerGeneration, String parquetFile, String directory) {
        WriterFileSet parquetSet = WriterFileSet.builder()
            .directory(java.nio.file.Path.of(directory))
            .writerGeneration(writerGeneration)
            .addFile(parquetFile)
            .addNumRows(1L)
            .build();
        Segment segment = Segment.builder(writerGeneration).addSearchableFiles("parquet", parquetSet).build();
        return new StubCatalogSnapshot(writerGeneration, List.of(segment));
    }

    private static final class StubCatalogSnapshot extends CatalogSnapshot {
        private final List<Segment> segments;

        StubCatalogSnapshot(long generation, List<Segment> segments) {
            super("stub", generation, 0L);
            this.segments = segments;
        }

        @Override
        protected void closeInternal() {}

        @Override
        public Map<String, String> getUserData() {
            return Map.of();
        }

        @Override
        public long getId() {
            return 0L;
        }

        @Override
        public List<Segment> getSegments() {
            return segments;
        }

        @Override
        public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
            return segments.stream().map(s -> s.dfGroupedSearchableFiles().get(dataFormat)).filter(fs -> fs != null).toList();
        }

        @Override
        public Set<String> getDataFormats() {
            return Set.of("parquet");
        }

        @Override
        public long getLastWriterGeneration() {
            return generation;
        }

        @Override
        public String serializeToString() {
            return "";
        }

        @Override
        public void setUserData(Map<String, String> userData, boolean commitData) {}

        @Override
        public CatalogSnapshot clone() {
            return this;
        }

        @Override
        public int getFormatVersionForFile(String file) {
            return 0;
        }

        @Override
        public byte[] serialize() {
            return new byte[0];
        }

        @Override
        public Collection<String> getFiles(boolean includeSegmentsFile) {
            return Collections.emptyList();
        }
    }

    /** Captures the requested row id and returns a fixed row map (or null). */
    private static final class StubExecutor implements GetService.NativeExecutor {
        private final Map<String, Object> row;
        long lastRowId = -1;

        StubExecutor(Map<String, Object> row) {
            this.row = row;
        }

        @Override
        public Map<String, Object> executeSingleRow(String parquetDir, String parquetFile, String tableName, long rowId) {
            this.lastRowId = rowId;
            if (row == null) return null;
            if (row.isEmpty()) return new HashMap<>();
            return new LinkedHashMap<>(row);
        }
    }
}
