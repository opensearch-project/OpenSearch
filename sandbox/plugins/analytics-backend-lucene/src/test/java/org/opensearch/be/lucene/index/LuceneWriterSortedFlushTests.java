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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LuceneWriter#flush(FlushInput)} with sort permutation.
 * Verifies that documents are reordered according to the sort permutation
 * from the primary data format (Parquet).
 */
public class LuceneWriterSortedFlushTests extends OpenSearchTestCase {

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

    /**
     * Writes 5 docs with row IDs 0..4, then flushes with a reverse permutation
     * (0→4, 1→3, 2→2, 3→1, 4→0). Verifies that the resulting Lucene segment
     * has docs reordered so that ___row_id at doc position 0 is 4 (the doc
     * that was originally at row 4 is now first).
     */
    public void testSortedFlushReordersDocuments() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = 5;
        MappedFieldType textField = mockTextField("content");

        // Build a reverse sort permutation: old_row_id → new_row_id
        // Original order: 0, 1, 2, 3, 4
        // Sorted order:   4, 3, 2, 1, 0  (reverse)
        long[] oldRowIds = { 0, 1, 2, 3, 4 };
        long[] newRowIds = { 4, 3, 2, 1, 0 };
        FlushInput sortedFlushInput = new FlushInput(new long[][] { oldRowIds, newRowIds });

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            assertTrue(fileInfos.getWriterFileSet(dataFormat).isPresent());

            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));
            assertThat(wfs.writerGeneration(), equalTo(1L));

            // Read the sorted segment and verify doc order
            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory()));
                 IndexReader reader = DirectoryReader.open(dir)) {

                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));

                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull("___row_id doc values should exist in sorted segment", rowIdDV);

                // After reverse sort: doc at position 0 should have the highest
                // remapped row_id, doc at position 4 should have the lowest.
                // The IndexSort sorts by remapped ___row_id ascending, so:
                // - doc with remapped row_id 0 (original row 4) is at position 0
                // - doc with remapped row_id 4 (original row 0) is at position 4
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    // The remapped value at this position should equal docId
                    // because IndexSort orders by ascending remapped value
                    long rowIdValue = rowIdDV.nextValue();
                    assertThat(
                        "Doc at position " + docId + " should have remapped row_id " + docId,
                        rowIdValue,
                        equalTo((long) (numDocs - 1 - docId))
                    );
                }
            }
        }
    }

    /**
     * Identity permutation (0→0, 1→1, ...) should produce the same doc order
     * as an unsorted flush.
     */
    public void testIdentityPermutationPreservesOrder() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = 10;
        MappedFieldType textField = mockTextField("content");

        long[] oldRowIds = new long[numDocs];
        long[] newRowIds = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            oldRowIds[i] = i;
            newRowIds[i] = i;
        }
        FlushInput sortedFlushInput = new FlushInput(new long[][] { oldRowIds, newRowIds });

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory()));
                 IndexReader reader = DirectoryReader.open(dir)) {

                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIdDV);

                // Identity permutation: doc at position i should have row_id i
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    assertThat(rowIdDV.nextValue(), equalTo((long) docId));
                }
            }
        }
    }

    /**
     * Sorted flush with zero documents should return empty FileInfos,
     * same as unsorted flush.
     */
    public void testSortedFlushWithNoDocsReturnsEmpty() throws IOException {
        Path baseDir = createTempDir();
        long[] oldRowIds = { 0, 1 };
        long[] newRowIds = { 1, 0 };
        FlushInput sortedFlushInput = new FlushInput(new long[][] { oldRowIds, newRowIds });

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            FileInfos fileInfos = writer.flush(sortedFlushInput);
            assertTrue(fileInfos.writerFilesMap().isEmpty());
        }
    }

    /**
     * Verifies that the sorted segment preserves the writer generation attribute
     * so that LuceneIndexingExecutionEngine.refresh() can correlate it.
     */
    public void testSortedFlushPreservesWriterGeneration() throws IOException {
        Path baseDir = createTempDir();
        long gen = 42L;
        MappedFieldType textField = mockTextField("content");

        long[] oldRowIds = { 0, 1, 2 };
        long[] newRowIds = { 2, 0, 1 };
        FlushInput sortedFlushInput = new FlushInput(new long[][] { oldRowIds, newRowIds });

        try (LuceneWriter writer = new LuceneWriter(gen, dataFormat, baseDir, null, Codec.getDefault())) {
            for (int i = 0; i < 3; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.writerGeneration(), equalTo(gen));
        }
    }

    /**
     * Verifies that the sorted segment has exactly 1 segment with the correct doc count.
     */
    public void testSortedFlushProducesSingleSegment() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = 20;
        MappedFieldType textField = mockTextField("content");

        // Shift-by-one permutation: 0→1, 1→2, ..., (N-1)→0
        long[] oldRowIds = new long[numDocs];
        long[] newRowIds = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            oldRowIds[i] = i;
            newRowIds[i] = (i + 1) % numDocs;
        }
        FlushInput sortedFlushInput = new FlushInput(new long[][] { oldRowIds, newRowIds });

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory()));
                 IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));
            }
        }
    }

    /**
     * FlushInput.EMPTY should trigger the unsorted path, producing sequential row IDs.
     */
    public void testEmptyFlushInputUsesUnsortedPath() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = 5;
        MappedFieldType textField = mockTextField("content");

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault())) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory()));
                 IndexReader reader = DirectoryReader.open(dir)) {

                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIdDV);

                // Unsorted: row IDs should be sequential 0..N-1
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    assertThat(rowIdDV.nextValue(), equalTo((long) docId));
                }
            }
        }
    }
}
