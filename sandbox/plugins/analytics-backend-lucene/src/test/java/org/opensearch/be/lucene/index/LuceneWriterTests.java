/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LuceneWriter} — the per-generation Lucene writer that creates
 * segments in isolated temp directories with force-merge to 1 segment on flush.
 */
public class LuceneWriterTests extends OpenSearchTestCase {

    private LuceneDataFormat dataFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataFormat = new LuceneDataFormat();
    }

    private MappedFieldType mockTextField(String name) {
        MappedFieldType ft = mock(MappedFieldType.class);
        when(ft.typeName()).thenReturn("text");
        when(ft.name()).thenReturn(name);
        return ft;
    }

    private MappedFieldType mockKeywordField(String name) {
        MappedFieldType ft = mock(MappedFieldType.class);
        when(ft.typeName()).thenReturn("keyword");
        when(ft.name()).thenReturn(name);
        when(ft.hasDocValues()).thenReturn(true);
        return ft;
    }

    public void testAddDocAndFlushProducesSingleSegment() throws IOException {
        Path baseDir = createTempDir();
        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            int numDocs = randomIntBetween(5, 20);
            MappedFieldType textField = mockTextField("content");
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "value " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                WriteResult result = writer.addDoc(input);
                assertTrue(result instanceof WriteResult.Success);
            }

            FileInfos fileInfos = writer.flush();
            assertTrue(fileInfos.getWriterFileSet(dataFormat).isPresent());

            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));
            assertThat(wfs.writerGeneration(), equalTo(1L));
            assertFalse(wfs.files().isEmpty());

            // Verify the segment is readable and has exactly numDocs documents
            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));
            }
        }
    }

    public void testRowIdMatchesLuceneDocId() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = randomIntBetween(10, 50);
        MappedFieldType textField = mockTextField("content");
        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader leafReader = ctx.reader();
                    NumericDocValues rowIdValues = leafReader.getNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                    assertNotNull("row_id doc values should exist", rowIdValues);
                    for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
                        assertTrue(rowIdValues.advanceExact(docId));
                        assertThat("row ID should equal Lucene doc ID", rowIdValues.longValue(), equalTo((long) docId));
                    }
                }
            }
        }
    }

    public void testFlushWithNoDocsReturnsEmpty() throws IOException {
        Path baseDir = createTempDir();
        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            FileInfos fileInfos = writer.flush();
            assertTrue(fileInfos.writerFilesMap().isEmpty());
        }
    }

    public void testWriterGenerationIsPreserved() throws IOException {
        Path baseDir = createTempDir();
        long gen = randomLongBetween(1, 100);
        MappedFieldType textField = mockTextField("content");
        try (LuceneWriter writer = new LuceneWriter(gen, dataFormat, baseDir, null, Codec.getDefault())) {
            assertThat(writer.generation(), equalTo(gen));

            LuceneDocumentInput input = new LuceneDocumentInput();
            input.addField(textField, "test value");
            input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
            writer.addDoc(input);

            FileInfos fileInfos = writer.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.writerGeneration(), equalTo(gen));
        }
    }

    public void testKeywordFieldsAreIndexed() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType keywordField = mockKeywordField("status");
        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            LuceneDocumentInput input = new LuceneDocumentInput();
            input.addField(keywordField, "active");
            input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
            writer.addDoc(input);

            FileInfos fileInfos = writer.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                assertThat(searcher.count(new MatchAllDocsQuery()), equalTo(1));
            }
        }
    }

    public void testUnsupportedFieldTypeIsSilentlySkipped() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType numericField = mock(MappedFieldType.class);
        when(numericField.typeName()).thenReturn("integer");
        when(numericField.name()).thenReturn("count");

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            LuceneDocumentInput input = new LuceneDocumentInput();
            // Should not throw — unsupported types are silently skipped (handled by other formats)
            input.addField(numericField, 42);
            // The document should have no fields for the unsupported type
            assertEquals(0, input.getFinalInput().getFields().size());
        }
    }

    public void testMixedTextAndKeywordFields() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("title");
        MappedFieldType keywordField = mockKeywordField("category");

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "document title " + i);
                input.addField(keywordField, "cat_" + (i % 3));
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));
            }
        }
    }

    public void testLockUnlock() throws IOException {
        Path baseDir = createTempDir();
        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            assertTrue(writer.tryLock());
            writer.unlock();
            writer.lock();
            writer.unlock();
        }
    }

    public void testWriteAndFlushEndToEndWithTextAndKeyword() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("body");
        MappedFieldType keywordField = mockKeywordField("status");
        int numDocs = randomIntBetween(5, 20);

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "hello world " + i);
                input.addField(keywordField, "active");
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                // Verify exactly 1 segment
                assertThat(reader.leaves().size(), equalTo(1));
                // Verify correct doc count
                assertThat(reader.numDocs(), equalTo(numDocs));

                // Verify row IDs match doc IDs
                LeafReader leafReader = reader.leaves().get(0).reader();
                NumericDocValues rowIdValues = leafReader.getNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIdValues);
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdValues.advanceExact(docId));
                    assertThat(rowIdValues.longValue(), equalTo((long) docId));
                }

                // Verify text field is searchable via TermQuery
                IndexSearcher searcher = new IndexSearcher(reader);
                assertTrue(searcher.count(new TermQuery(new Term("body", "hello"))) > 0);

                // Verify keyword field is searchable
                assertTrue(searcher.count(new TermQuery(new Term("status", "active"))) > 0);
            }
        }
    }

    public void testMultipleWriterGenerationsProduceIsolatedSegments() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("content");

        long gen1 = 1L;
        long gen2 = 2L;
        int numDocs1 = randomIntBetween(3, 10);
        int numDocs2 = randomIntBetween(3, 10);

        FileInfos fileInfos1;
        FileInfos fileInfos2;

        // Create both writers without closing them until after verification,
        // because close() deletes the temp directory.
        LuceneWriter writer1 = new LuceneWriter(gen1, dataFormat, baseDir, null, Codec.getDefault());
        LuceneWriter writer2 = new LuceneWriter(gen2, dataFormat, baseDir, null, Codec.getDefault());
        try {
            for (int i = 0; i < numDocs1; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "gen1 doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer1.addDoc(input);
            }
            fileInfos1 = writer1.flush();

            for (int i = 0; i < numDocs2; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "gen2 doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer2.addDoc(input);
            }
            fileInfos2 = writer2.flush();

            // Verify each produces its own independent segment
            WriterFileSet wfs1 = fileInfos1.getWriterFileSet(dataFormat).get();
            WriterFileSet wfs2 = fileInfos2.getWriterFileSet(dataFormat).get();

            // Different directories
            assertNotEquals("Writers should have different directories", wfs1.directory(), wfs2.directory());

            // Correct generations
            assertThat(wfs1.writerGeneration(), equalTo(gen1));
            assertThat(wfs2.writerGeneration(), equalTo(gen2));

            // Correct doc counts
            assertThat(wfs1.numRows(), equalTo((long) numDocs1));
            assertThat(wfs2.numRows(), equalTo((long) numDocs2));

            // Each is independently readable with correct content
            try (NIOFSDirectory dir1 = new NIOFSDirectory(Path.of(wfs1.directory())); IndexReader reader1 = DirectoryReader.open(dir1)) {
                assertThat(reader1.numDocs(), equalTo(numDocs1));
                assertThat(reader1.leaves().size(), equalTo(1));
            }

            try (NIOFSDirectory dir2 = new NIOFSDirectory(Path.of(wfs2.directory())); IndexReader reader2 = DirectoryReader.open(dir2)) {
                assertThat(reader2.numDocs(), equalTo(numDocs2));
                assertThat(reader2.leaves().size(), equalTo(1));
            }
        } finally {
            writer1.close();
            writer2.close();
        }
    }
}
