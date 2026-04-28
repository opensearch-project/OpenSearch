/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.be.lucene.merge.LuceneMerger;
import org.opensearch.be.lucene.merge.RowIdRemappingSortField;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end tests for {@link LuceneMerger}.
 *
 * <p>These tests create real Lucene segments with {@code writer_generation} attributes
 * and {@code ___row_id} doc values, then exercise the merge path and validate the output.
 */
public class LuceneMergerTests extends OpenSearchTestCase {

    private static final String ROW_ID_FIELD = "___row_id";
    private static final String WRITER_GENERATION_ATTR = "writer_generation";

    private MergeIndexWriter writer;
    private Directory directory;
    private RowIdRemappingSortField sortField;
    private Path dataPath;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataPath = createTempDir();
        directory = NIOFSDirectory.open(dataPath);
        sortField = new RowIdRemappingSortField(ROW_ID_FIELD);
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setIndexSort(new Sort(sortField));
        writer = new MergeIndexWriter(directory, iwc);
    }

    @Override
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
        super.tearDown();
    }

    // ========== Test Cases ==========

    /**
     * Merge with empty input returns empty result without error.
     */
    public void testMergeWithEmptyInput() throws IOException {
        LuceneMerger merger = new LuceneMerger(writer, sortField, new LuceneDataFormat(), dataPath);
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(99L).build();

        MergeResult result = merger.merge(input);
        assertNotNull(result);
        assertTrue(result.getMergedWriterFileSet().isEmpty());
    }

    /**
     * Merge with no matching segments returns empty result and logs warning.
     */
    public void testMergeWithNoMatchingSegments() throws IOException {
        writeSegment(writer, 1L, 0, 3);
        writer.commit();

        LuceneMerger merger = new LuceneMerger(writer, sortField, new LuceneDataFormat(), dataPath);

        Segment segment = Segment.builder(99L).build();
        MergeInput input = MergeInput.builder().addSegment(segment).newWriterGeneration(100L).build();

        MergeResult result = merger.merge(input);
        assertNotNull(result);
        assertTrue(result.getMergedWriterFileSet().isEmpty());
    }

    /**
     * Merge without RowIdMapping (primary merge) preserves all documents and field data.
     */
    public void testMergeWithoutRowIdMappingPreservesData() throws IOException {
        writeSegment(writer, 1L, 0, 3);
        writeSegment(writer, 2L, 3, 2);
        writer.commit();

        SegmentInfos infos = getSegmentInfos(writer);
        assertEquals(2, infos.size());
        assertEquals(5, writer.getDocStats().numDocs);

        LuceneMerger merger = new LuceneMerger(writer, sortField, new LuceneDataFormat(), dataPath);
        List<Segment> segments = buildSegments(infos);

        MergeInput input = MergeInput.builder().segments(segments).newWriterGeneration(10L).build();
        MergeResult result = merger.merge(input);
        assertNotNull(result);

        writer.commit();
        // addIndexes adds a new merged segment; original segments remain.
        // Total docs = original 5 + merged 5 = 10
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            assertTrue("Should have at least 5 docs after merge", reader.numDocs() >= 5);
            Set<String> foundIds = new HashSet<>();
            for (LeafReaderContext ctx : reader.leaves()) {
                for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                    Document doc = ctx.reader().storedFields().document(i);
                    foundIds.add(doc.get("id"));
                    assertNotNull("data field should be preserved", doc.get("data"));
                }
            }
            for (int i = 0; i < 5; i++) {
                assertTrue("Missing doc id: doc_" + i, foundIds.contains("doc_" + i));
            }
        }
    }

    /**
     * Merge with RowIdMapping remaps ___row_id doc values AND reorders documents.
     * Verifies that the merged segment has documents sorted by remapped row IDs
     * and that stored fields follow the documents to their new positions.
     *
     * The mapping preserves within-segment order (ascending remapped values within
     * each generation), matching real Parquet merge behavior where rows within each
     * source file maintain their relative order in the merged output.
     */
    public void testMergeWithRowIdMappingRemapsRowIds() throws IOException {
        // gen=1: doc_0 (rowId=0), doc_1 (rowId=1), doc_2 (rowId=2)
        // gen=2: doc_3 (rowId=0), doc_4 (rowId=1)
        writeSegment(writer, 1L, 0, 3);
        writeSegment(writer, 2L, 3, 2);
        writer.commit();

        assertEquals(5, writer.getDocStats().numDocs);

        // Mapping interleaves segments but preserves within-segment order:
        // gen=1: 0→0, 1→2, 2→4 (ascending within gen=1)
        // gen=2: 0→1, 1→3 (ascending within gen=2)
        //
        // This simulates a Parquet merge that interleaves rows from two files:
        // merged output: gen1-row0, gen2-row0, gen1-row1, gen2-row1, gen1-row2
        //
        // Expected sorted order by remapped rowId:
        // position 0: rowId=0 → doc_0 (gen=1, original rowId=0)
        // position 1: rowId=1 → doc_3 (gen=2, original rowId=0)
        // position 2: rowId=2 → doc_1 (gen=1, original rowId=1)
        // position 3: rowId=3 → doc_4 (gen=2, original rowId=1)
        // position 4: rowId=4 → doc_2 (gen=1, original rowId=2)
        Map<Long, Map<Long, Long>> mapping = new HashMap<>();
        mapping.put(1L, Map.of(0L, 0L, 1L, 2L, 2L, 4L));
        mapping.put(2L, Map.of(0L, 1L, 1L, 3L));
        RowIdMapping rowIdMapping = (oldId, oldGeneration) -> {
            Map<Long, Long> genMap = mapping.get(oldGeneration);
            if (genMap != null && genMap.containsKey(oldId)) {
                return genMap.get(oldId);
            }
            return oldId;
        };

        LuceneMerger merger = new LuceneMerger(writer, sortField, new LuceneDataFormat(), dataPath);
        SegmentInfos infos = getSegmentInfos(writer);
        List<Segment> segments = buildSegments(infos);

        MergeInput input = MergeInput.builder().segments(segments).rowIdMapping(rowIdMapping).newWriterGeneration(10L).build();

        MergeResult result = merger.merge(input);
        assertNotNull(result);
        assertTrue(result.rowIdMapping().isPresent());

        writer.commit();

        // Expected: documents sorted by remapped rowId, with correct stored fields
        String[] expectedIds = { "doc_0", "doc_3", "doc_1", "doc_4", "doc_2" };
        long[] expectedRowIds = { 0, 1, 2, 3, 4 };

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            // Find the merged segment (should be the largest leaf after old segments are deleted)
            LeafReaderContext mergedLeaf = null;
            for (LeafReaderContext ctx : reader.leaves()) {
                if (mergedLeaf == null || ctx.reader().maxDoc() > mergedLeaf.reader().maxDoc()) {
                    mergedLeaf = ctx;
                }
            }
            assertNotNull("Should have at least one leaf", mergedLeaf);
            assertEquals("Merged segment should have 5 docs", 5, mergedLeaf.reader().maxDoc());

            SortedNumericDocValues rowIdDV = mergedLeaf.reader().getSortedNumericDocValues(ROW_ID_FIELD);
            assertNotNull("___row_id doc values should exist", rowIdDV);

            for (int i = 0; i < 5; i++) {
                // Verify ___row_id value
                assertTrue("Should have doc values for doc " + i, rowIdDV.advanceExact(i));
                long actualRowId = rowIdDV.nextValue();
                assertEquals("Doc at position " + i + " should have ___row_id=" + expectedRowIds[i], expectedRowIds[i], actualRowId);

                // Verify stored field follows the document
                Document doc = mergedLeaf.reader().storedFields().document(i);
                assertEquals("Doc at position " + i + " should be " + expectedIds[i], expectedIds[i], doc.get("id"));
            }
        }
    }

    /**
     * Merge preserves keyword, numeric, and stored field data integrity.
     */
    public void testMergePreservesFieldDataIntegrity() throws IOException {
        writeSegmentWithRichFields(writer, 1L, 0, 3);
        writeSegmentWithRichFields(writer, 2L, 3, 2);
        writer.commit();

        LuceneMerger merger = new LuceneMerger(writer, sortField, new LuceneDataFormat(), dataPath);
        SegmentInfos infos = getSegmentInfos(writer);
        List<Segment> segments = buildSegments(infos);

        MergeInput input = MergeInput.builder().segments(segments).newWriterGeneration(10L).build();
        merger.merge(input);
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            assertTrue("Should have at least 5 docs after merge", reader.numDocs() >= 5);
            for (LeafReaderContext ctx : reader.leaves()) {
                for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                    Document doc = ctx.reader().storedFields().document(i);
                    String id = doc.get("id");
                    assertNotNull("id field missing", id);
                    String storedData = doc.get("data");
                    assertNotNull("stored data field missing for " + id, storedData);
                    assertTrue("data should contain the doc id", storedData.contains(id));
                    String numericStr = doc.get("score");
                    assertNotNull("stored numeric field missing for " + id, numericStr);
                }
            }
        }
    }

    /**
     * Constructor with null IndexWriter throws IllegalArgumentException.
     */
    public void testConstructorWithNullIndexWriterThrows() {
        expectThrows(IllegalArgumentException.class, () -> new LuceneMerger(null, sortField, new LuceneDataFormat(), Path.of(".")));
    }

    // ========== Helper Methods ==========

    private void writeSegment(IndexWriter w, long generation, int startRowId, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "doc_" + (startRowId + i), Field.Store.YES));
            doc.add(new StoredField("data", "value_for_doc_" + (startRowId + i)));
            // ___row_id is local to the segment: 0, 1, 2, ... (matches how the real system works)
            doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, i));
            w.addDocument(doc);
        }
        w.flush();
        setWriterGenerationOnLatestSegment(w, generation);
    }

    private void writeSegmentWithRichFields(IndexWriter w, long generation, int startRowId, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            int docIdx = startRowId + i;
            Document doc = new Document();
            doc.add(new StringField("id", "doc_" + docIdx, Field.Store.YES));
            doc.add(new StoredField("data", "rich_data_for_doc_" + docIdx));
            doc.add(new StoredField("score", String.valueOf(docIdx * 10)));
            doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, docIdx));
            doc.add(new SortedNumericDocValuesField("score_dv", docIdx * 10));
            w.addDocument(doc);
        }
        w.flush();
        setWriterGenerationOnLatestSegment(w, generation);
    }

    @SuppressForbidden(reason = "Need reflection to stamp writer_generation on segments for testing")
    private void setWriterGenerationOnLatestSegment(IndexWriter w, long generation) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segInfos = (SegmentInfos) segInfosField.get(w);
            if (segInfos.size() > 0) {
                SegmentCommitInfo lastSegment = segInfos.asList().get(segInfos.size() - 1);
                if (lastSegment.info.getAttribute(WRITER_GENERATION_ATTR) == null) {
                    lastSegment.info.putAttribute(WRITER_GENERATION_ATTR, String.valueOf(generation));
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to set writer_generation attribute via reflection", e);
        }
    }

    @SuppressForbidden(reason = "Need reflection to access live SegmentInfos for test assertions")
    private SegmentInfos getSegmentInfos(IndexWriter w) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            return (SegmentInfos) segInfosField.get(w);
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to access segmentInfos via reflection", e);
        }
    }

    private List<Segment> buildSegments(SegmentInfos infos) {
        List<Segment> segments = new ArrayList<>();
        for (SegmentCommitInfo sci : infos.asList()) {
            String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTR);
            if (genAttr != null) {
                long generation = Long.parseLong(genAttr);
                segments.add(Segment.builder(generation).build());
            }
        }
        return segments;
    }
}
