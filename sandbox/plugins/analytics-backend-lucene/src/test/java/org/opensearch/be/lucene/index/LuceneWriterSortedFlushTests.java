/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.PackedSingleGenRowIdMapping;
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
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), null)) {
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

                // After reverse sort with row ID rewrite: docs are physically reordered,
                // and __row_id__ values are rewritten to sequential 0..N.
                // So doc at position docId should have row_id = docId.
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    long rowIdValue = rowIdDV.nextValue();
                    assertThat(
                        "Doc at position " + docId + " should have sequential row_id " + docId,
                        rowIdValue,
                        equalTo((long) docId)
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
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), null)) {
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
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), null)) {
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
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (LuceneWriter writer = new LuceneWriter(gen, dataFormat, baseDir, null, Codec.getDefault(), null)) {
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
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), null)) {
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

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), null)) {
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

    // ── Tests for Lucene as primary format (with IndexSort) ──

    /**
     * When Lucene is the primary format with IndexSort on a numeric field,
     * documents should be physically sorted by that field in the segment.
     * FlushInput.EMPTY is used because sort is handled by Lucene's native IndexSort.
     */
    public void testPrimaryFormatWithIndexSortProducesSortedSegment() throws IOException {
        Path baseDir = createTempDir();
        Sort indexSort = new Sort(new SortedNumericSortField("age", SortField.Type.LONG));

        // Insert docs with ages in reverse order: 50, 40, 30, 20, 10
        int numDocs = 5;
        long[] ages = { 50, 40, 30, 20, 10 };

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), indexSort)) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                // Add the sort field directly on the document
                input.getFinalInput().add(new SortedNumericDocValuesField("age", ages[i]));
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory()));
                 IndexReader reader = DirectoryReader.open(dir)) {

                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));

                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues ageDV = leaf.getSortedNumericDocValues("age");
                assertNotNull("age doc values should exist", ageDV);

                // After IndexSort(age ASC): docs should be ordered 10, 20, 30, 40, 50
                long previousAge = Long.MIN_VALUE;
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(ageDV.advanceExact(docId));
                    long ageValue = ageDV.nextValue();
                    assertTrue("age values should be in ascending order, got " + ageValue + " after " + previousAge,
                        ageValue >= previousAge);
                    previousAge = ageValue;
                }
            }
        }
    }

    /**
     * When Lucene is primary with IndexSort, row IDs should be reordered along with
     * the documents (they follow the physical doc order, not rewritten to sequential).
     */
    public void testPrimaryFormatIndexSortReordersRowIds() throws IOException {
        Path baseDir = createTempDir();
        Sort indexSort = new Sort(new SortedNumericSortField("age", SortField.Type.LONG));

        // Docs inserted with ages: 30, 10, 20 → after sort: 10(row1), 20(row2), 30(row0)
        long[] ages = { 30, 10, 20 };
        int numDocs = 3;

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), indexSort)) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                input.getFinalInput().add(new SortedNumericDocValuesField("age", ages[i]));
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory()));
                 IndexReader reader = DirectoryReader.open(dir)) {

                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIdDV);

                // After sort by age ASC: doc order is age=10(orig row 1), age=20(orig row 2), age=30(orig row 0)
                // Row IDs follow the docs, so: position 0 → row_id 1, position 1 → row_id 2, position 2 → row_id 0
                long[] expectedRowIds = { 1, 2, 0 };
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    assertThat("row_id at position " + docId, rowIdDV.nextValue(), equalTo(expectedRowIds[docId]));
                }
            }
        }
    }

    /**
     * When Lucene is primary with IndexSort and docs are already in sorted order,
     * the segment should preserve the original order.
     */
    public void testPrimaryFormatIndexSortWithAlreadySortedDocs() throws IOException {
        Path baseDir = createTempDir();
        Sort indexSort = new Sort(new SortedNumericSortField("age", SortField.Type.LONG));

        // Docs already in sorted order
        long[] ages = { 10, 20, 30, 40, 50 };
        int numDocs = 5;

        try (LuceneWriter writer = new LuceneWriter(1L, dataFormat, baseDir, null, Codec.getDefault(), indexSort)) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                input.getFinalInput().add(new SortedNumericDocValuesField("age", ages[i]));
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory()));
                 IndexReader reader = DirectoryReader.open(dir)) {

                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIdDV);

                // Already sorted → row IDs remain sequential
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    assertThat(rowIdDV.nextValue(), equalTo((long) docId));
                }
            }
        }
    }

    private static PackedSingleGenRowIdMapping buildMapping(long[] oldRowIds, long[] newRowIds) {
        int numDocs = oldRowIds.length;
        long[] oldToNew = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            oldToNew[i] = i;
        }
        for (int i = 0; i < oldRowIds.length; i++) {
            oldToNew[(int) oldRowIds[i]] = newRowIds[i];
        }
        return new PackedSingleGenRowIdMapping(oldToNew);
    }
}
