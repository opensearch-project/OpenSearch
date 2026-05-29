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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.equalTo;

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
        return new TextFieldMapper.TextFieldType(name);
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
        // Sorted order: 4, 3, 2, 1, 0 (reverse)
        long[] oldRowIds = { 0, 1, 2, 3, 4 };
        long[] newRowIds = { 4, 3, 2, 1, 0 };
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet()
            )
        ) {
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
            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {

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
                    assertThat("Doc at position " + docId + " should have sequential row_id " + docId, rowIdValue, equalTo((long) docId));
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

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet()
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {

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

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet()
            )
        ) {
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

        try (
            LuceneWriter writer = new LuceneWriter(
                gen,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet()
            )
        ) {
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

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet()
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));
            }
        }
    }

    /**
     * FlushInput.EMPTY should trigger the unsorted path, leaving docs in insertion order.
     * To prove no reordering happened, each doc carries a {@code marker} doc value equal
     * to {@code rowId * 100}. After flush, both {@code __row_id__} and {@code marker} must
     * appear at the same position in doc order — confirming the segment is identical to
     * the insertion sequence and that the reorder merge policy was never installed.
     */
    public void testEmptyFlushInputUsesUnsortedPath() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = 5;
        MappedFieldType textField = mockTextField("content");
        // When no user sort is configured, the writer still needs a row_id IndexSort
        // (matching what LuceneCommitter sets in production for the unsorted primary case).
        Sort rowIdSort = new Sort(new SortedNumericSortField(LuceneDocumentInput.ROW_ID_FIELD, SortField.Type.LONG));

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                rowIdSort,
                ConcurrentHashMap.newKeySet()
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                // Marker doc value uniquely tied to row ID — would diverge from row ID
                // if any reordering had taken place.
                input.getFinalInput().add(new SortedNumericDocValuesField("marker", i * 100L));
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {

                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                SortedNumericDocValues markerDV = leaf.getSortedNumericDocValues("marker");
                assertNotNull(rowIdDV);
                assertNotNull(markerDV);

                // Unsorted: row IDs sequential 0..N-1 AND marker[docId] == docId*100,
                // proving doc at position docId is the doc originally inserted with row ID docId.
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    assertThat(rowIdDV.nextValue(), equalTo((long) docId));

                    assertTrue(markerDV.advanceExact(docId));
                    assertThat(
                        "marker at position " + docId + " must equal " + (docId * 100L) + " (no reordering)",
                        markerDV.nextValue(),
                        equalTo(docId * 100L)
                    );
                }
            }
        }
    }

    // ── Tests for Lucene as primary format (with IndexSort) ──

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

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                indexSort,
                ConcurrentHashMap.newKeySet()
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                input.getFinalInput().add(new SortedNumericDocValuesField("age", ages[i]));
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {

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

    /**
     * When the child IndexWriter is configured with an IndexSort (Lucene as primary),
     * a FlushInput carrying a RowIdMapping is contradictory — sorting would be applied
     * twice (once natively by Lucene's IndexSort, once via the row ID permutation).
     * The flush call must fail fast with an IllegalStateException whose message
     * names the configured sort and the writer generation, so the misconfiguration
     * is easy to attribute.
     */
    public void testFlushFailsWhenIndexSortAndRowIdMappingAreBothConfigured() throws IOException {
        Path baseDir = createTempDir();
        long generation = 7L;
        Sort indexSort = new Sort(new SortedNumericSortField("age", SortField.Type.LONG));
        MappedFieldType textField = mockTextField("content");

        long[] oldRowIds = { 0, 1, 2 };
        long[] newRowIds = { 2, 0, 1 };
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (
            LuceneWriter writer = new LuceneWriter(
                generation,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                indexSort,
                ConcurrentHashMap.newKeySet()
            )
        ) {
            for (int i = 0; i < oldRowIds.length; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                input.getFinalInput().add(new SortedNumericDocValuesField("age", i));
                writer.addDoc(input);
            }

            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> writer.flush(sortedFlushInput));
            assertThat(
                "exception message should reference RowIdMapping",
                ex.getMessage(),
                org.hamcrest.Matchers.containsString("RowIdMapping")
            );
            assertThat(
                "exception message should include the configured IndexSort",
                ex.getMessage(),
                org.hamcrest.Matchers.containsString(indexSort.toString())
            );
            assertThat(
                "exception message should include the writer generation",
                ex.getMessage(),
                org.hamcrest.Matchers.containsString(Long.toString(generation))
            );
        }
    }

    /**
     * After a sorted flush with reorder + row ID rewrite, non-row-id fields must be
     * physically reordered to follow the docs (their values stay attached to their
     * original docs, just at new positions), while the {@code ___row_id} field is
     * rewritten to sequential 0..N-1 in the new doc order. This guarantees:
     *   - cross-format alignment with Parquet (sequential row IDs at each doc position)
     *   - user-field correctness (no value loss or reassignment)
     */
    public void testSortedFlushReordersOtherFieldsAndRewritesRowIds() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = 6;
        MappedFieldType textField = mockTextField("content");

        // Full reverse permutation: 0→5, 1→4, ..., 5→0
        long[] oldRowIds = new long[numDocs];
        long[] newRowIds = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            oldRowIds[i] = i;
            newRowIds[i] = numDocs - 1 - i;
        }
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet()
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                // User field: doc i carries score i*100
                input.getFinalInput().add(new SortedNumericDocValuesField("score", i * 100L));
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {

                LeafReader leaf = reader.leaves().get(0).reader();

                // Row IDs are rewritten to sequential 0..N-1 in the new doc order
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIdDV);
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdDV.advanceExact(docId));
                    assertThat(rowIdDV.nextValue(), equalTo((long) docId));
                }

                // Score field is reordered (not rewritten): new doc at position d came
                // from original position (numDocs-1-d), so it carries score (numDocs-1-d)*100.
                SortedNumericDocValues scoreDV = leaf.getSortedNumericDocValues("score");
                assertNotNull(scoreDV);
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(scoreDV.advanceExact(docId));
                    long expectedScore = (numDocs - 1 - docId) * 100L;
                    assertThat(
                        "score at new position " + docId + " should be " + expectedScore,
                        scoreDV.nextValue(),
                        equalTo(expectedScore)
                    );
                }
            }
        }
    }

    /**
     * Sorted flush across multiple in-flight Lucene segments.
     *
     * <p>By default {@link LuceneWriter} disables doc-count flushing and uses a 256 MB
     * RAM buffer, so all docs land in a single in-memory segment before flush. To
     * exercise the multi-segment merge path — where the configured
     * {@code ReorderingMergePolicy} drives a reorder that spans more than one source
     * segment — the test overrides the package-private hooks: disables RAM-buffer
     * flushing and sets {@code maxBufferedDocs()} to a small value (20), which makes
     * Lucene spill a new segment every 20 docs deterministically.
     *
     * <p>After {@code flush(FlushInput)} with a reverse permutation:
     *   - {@code ___row_id} doc values must be sequential 0..N-1 (rewritten in the merge)
     *   - the {@code marker} side-channel must follow the reverse permutation, proving
     *     the reorder actually moved docs and is not just an effect of natural ordering.
     */
    public void testSortedFlushAcrossMultipleSegmentsReordersAndRewritesRowIds() throws IOException {
        // Default: a single segment is produced before flush, no inter-segment merge needed.
        runSortedFlushAndAssert(/* maxBufferedDocsOverride= */ null);

        // Force multiple buffered segments deterministically: 200 docs / 20 per buffer
        // = 10 in-flight segments before forceMerge(1).
        runSortedFlushAndAssert(/* maxBufferedDocsOverride= */ 20);
    }

    /**
     * Reusable scenario: index N docs with a {@code marker} side-channel in arrival order,
     * flush with a reverse permutation, then assert reorder + row ID rewrite.
     *
     * @param maxBufferedDocsOverride non-null to override the writer's default max-buffered-docs
     *                                via the package-private {@code maxBufferedDocs()} hook
     *                                (RAM-buffer flushing is disabled in that case)
     */
    private void runSortedFlushAndAssert(Integer maxBufferedDocsOverride) throws IOException {
        Path baseDir = createTempDir();
        // Enough docs that with a small maxBufferedDocs Lucene is forced to spill multiple segments.
        int numDocs = 200;
        MappedFieldType textField = mockTextField("content");

        // Reverse permutation: doc inserted at position i ends up at position (N-1-i)
        long[] oldRowIds = new long[numDocs];
        long[] newRowIds = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            oldRowIds[i] = i;
            newRowIds[i] = numDocs - 1 - i;
        }
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (LuceneWriter writer = newWriterWithMaxBufferedDocs(baseDir, maxBufferedDocsOverride)) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                input.getFinalInput().add(new SortedNumericDocValuesField("marker", i * 100L));
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(sortedFlushInput);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {

                // Single committed segment after forceMerge(1) regardless of how many
                // intermediate segments existed during indexing.
                assertThat(reader.leaves().size(), equalTo(1));
                LeafReader leaf = reader.leaves().get(0).reader();
                assertThat(leaf.maxDoc(), equalTo(numDocs));

                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                SortedNumericDocValues markerDV = leaf.getSortedNumericDocValues("marker");
                assertNotNull(rowIdDV);
                assertNotNull(markerDV);

                for (int docId = 0; docId < numDocs; docId++) {
                    // Row IDs rewritten to sequential 0..N-1 in the post-reorder doc order.
                    assertTrue(rowIdDV.advanceExact(docId));
                    assertThat(rowIdDV.nextValue(), equalTo((long) docId));

                    // Marker side-channel proves the reverse reorder fired: doc at new
                    // position docId came from original position (N-1-docId), so its
                    // marker is (N-1-docId)*100.
                    assertTrue(markerDV.advanceExact(docId));
                    long expectedMarker = (numDocs - 1 - docId) * 100L;
                    assertThat(
                        "marker at position " + docId + " should be " + expectedMarker,
                        markerDV.nextValue(),
                        equalTo(expectedMarker)
                    );
                }
            }
        }
    }

    /**
     * Constructs a {@link LuceneWriter} with an optional override of the package-private
     * {@code maxBufferedDocs()} hook. Pass {@code null} for the default (single segment via
     * RAM buffer) or a small value (e.g. 20) to force deterministic multi-segment creation.
     * When overriding, RAM-buffer flushing is disabled so doc count is the sole trigger.
     */
    private LuceneWriter newWriterWithMaxBufferedDocs(Path baseDir, Integer maxBufferedDocsOverride) throws IOException {
        if (maxBufferedDocsOverride == null) {
            return new LuceneWriter(1L, 0L, dataFormat, baseDir, null, Codec.getDefault(), null, ConcurrentHashMap.newKeySet());
        }
        final int override = maxBufferedDocsOverride;
        return new LuceneWriter(1L, 0L, dataFormat, baseDir, null, Codec.getDefault(), null, ConcurrentHashMap.newKeySet()) {
            @Override
            protected double ramBufferSizeMB() {
                return 1024.0;
            }

            @Override
            protected int maxBufferedDocs() {
                return override;
            }
        };
    }

    private static PackedRowIdMapping buildMapping(long[] oldRowIds, long[] newRowIds) {
        int numDocs = oldRowIds.length;
        long[] oldToNew = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            oldToNew[i] = i;
        }
        for (int i = 0; i < oldRowIds.length; i++) {
            oldToNew[(int) oldRowIds[i]] = newRowIds[i];
        }
        return new PackedRowIdMapping(oldToNew, true);
    }
}
