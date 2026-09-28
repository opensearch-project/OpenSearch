/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AbstractSortedRefreshIT extends OpenSearchIntegTestCase {

    protected static final String INDEX_NAME = "test-composite-refresh-sorted";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (Exception e) {
            // index may not exist
        }
        super.tearDown();
    }

    protected void createIndex(Settings settings) {
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).setMapping(getMapping()).get();
        ensureGreen(INDEX_NAME);
    }

    protected String[] getMapping() {
        return new String[] { "name", "type=keyword", "age", "type=integer", "tag", "type=keyword" };
    }

    protected void indexDoc(String name, int age) {
        IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", name, "age", age).get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    protected void indexDoc(String name, int age, String tag) {
        IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", name, "age", age, "tag", tag).get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    protected void indexDocNullAge(String name) {
        IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", name).get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    protected void flushAndRefresh() {
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        FlushResponse flushResponse = client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        assertEquals(RestStatus.OK, flushResponse.getStatus());
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: verification
    // ══════════════════════════════════════════════════════════════════════

    protected void verifyParquetRowCount(DataformatAwareCatalogSnapshot snapshot, int expectedTotalDocs) throws IOException {
        Path parquetDir = getParquetDir();
        long totalRows = 0;
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            assertNotNull("Segment should have parquet files", wfs);
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                assertTrue("Parquet file should exist: " + filePath, Files.exists(filePath));
                ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath.toString());
                totalRows += metadata.numRows();
            }
        }
        assertEquals("Total rows should match ingested docs", expectedTotalDocs, totalRows);
    }

    @SuppressForbidden(reason = "JSON parsing for sort order verification")
    protected void verifyParquetSortOrder(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        Path parquetDir = getParquetDir();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                String json = RustBridge.readAsJson(filePath.toString());
                List<Map<String, Object>> rows;
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    rows = parser.list().stream().map(o -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) o;
                        return m;
                    }).toList();
                }
                if (rows.size() <= 1) continue;

                for (int i = 1; i < rows.size(); i++) {
                    Object prevAge = rows.get(i - 1).get("age");
                    Object currAge = rows.get(i).get("age");

                    // nulls first for age
                    if (prevAge == null && currAge == null) continue;
                    if (prevAge == null) continue;
                    if (currAge == null) {
                        fail("age null should come before non-null at row " + i);
                    }

                    int prevAgeVal = ((Number) prevAge).intValue();
                    int currAgeVal = ((Number) currAge).intValue();

                    assertTrue(
                        "age should be DESC but found " + prevAgeVal + " before " + currAgeVal + " at row " + i + " in " + file,
                        prevAgeVal >= currAgeVal
                    );

                    // When age is equal, verify name ASC (nulls last)
                    if (prevAgeVal == currAgeVal) {
                        Object prevName = rows.get(i - 1).get("name");
                        Object currName = rows.get(i).get("name");

                        if (prevName != null && currName == null) continue;
                        if (prevName == null && currName != null) {
                            fail("name nulls should be last at row " + i + " in " + file);
                        }
                        if (prevName != null && currName != null) {
                            assertTrue(
                                "name should be ASC but found '" + prevName + "' before '" + currName + "' at row " + i + " in " + file,
                                ((String) prevName).compareTo((String) currName) <= 0
                            );
                        }
                    }
                }
            }
        }
    }

    protected void verifyLuceneDocCount(int expectedTotalDocs) throws IOException {
        Path luceneDir = getLuceneDir();
        assertTrue("Lucene directory should exist", Files.exists(luceneDir));
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Lucene doc count should match", expectedTotalDocs, reader.numDocs());
        }
    }

    /**
     * Verifies Parquet sort order for a dynamic set of integer sort fields.
     * Supports any combination of ASC/DESC with configurable null placement.
     */
    @SuppressForbidden(reason = "JSON parsing for sort order verification")
    protected void verifyParquetSortOrderMultiField(
        DataformatAwareCatalogSnapshot snapshot,
        String[] fieldNames,
        String[] sortOrders,
        String[] sortMissing
    ) throws Exception {
        Path parquetDir = getParquetDir();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                String json = RustBridge.readAsJson(filePath.toString());
                List<Map<String, Object>> rows;
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    rows = parser.list().stream().map(o -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) o;
                        return m;
                    }).toList();
                }
                if (rows.size() <= 1) continue;

                for (int i = 1; i < rows.size(); i++) {
                    int cmp = compareRowsByMultiField(rows.get(i - 1), rows.get(i), fieldNames, sortOrders, sortMissing);
                    assertTrue(
                        "Sort order violated at row " + i + " in " + file + ": prev=" + rows.get(i - 1) + " curr=" + rows.get(i),
                        cmp <= 0
                    );
                }
            }
        }
    }

    /**
     * Compares two rows by the multi-field sort key. Returns negative if prev comes before curr
     * (correct order), zero if equal, positive if prev comes after curr (violation).
     */
    protected int compareRowsByMultiField(
        Map<String, Object> prev,
        Map<String, Object> curr,
        String[] fieldNames,
        String[] sortOrders,
        String[] sortMissing
    ) {
        for (int f = 0; f < fieldNames.length; f++) {
            Object prevVal = prev.get(fieldNames[f]);
            Object currVal = curr.get(fieldNames[f]);
            boolean isAsc = "asc".equals(sortOrders[f]);
            boolean nullsFirst = "_first".equals(sortMissing[f]);

            int cmp = compareValues(prevVal, currVal, isAsc, nullsFirst);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    /**
     * Compares two nullable integer values according to sort direction and null placement.
     * Returns negative if in correct order, zero if equal, positive if violated.
     */
    protected int compareValues(Object prev, Object curr, boolean isAsc, boolean nullsFirst) {
        if (prev == null && curr == null) return 0;
        if (prev == null) {
            // prev is null: correct if nullsFirst, violation if nullsLast
            return nullsFirst ? -1 : 1;
        }
        if (curr == null) {
            // curr is null: violation if nullsFirst, correct if nullsLast
            return nullsFirst ? 1 : -1;
        }
        int prevInt = ((Number) prev).intValue();
        int currInt = ((Number) curr).intValue();
        int natural = Integer.compare(prevInt, currInt);
        return isAsc ? natural : -natural;
    }

    protected void verifyLuceneRowIdSequential() throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
                if (rowIdDV == null) continue;

                long expectedRowId = 0;
                for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                    if (rowIdDV.advanceExact(doc)) {
                        long rowId = rowIdDV.nextValue();
                        assertEquals(
                            DocumentInput.ROW_ID_FIELD
                                + " should be sequential, expected "
                                + expectedRowId
                                + " but got "
                                + rowId
                                + " at doc "
                                + doc,
                            expectedRowId,
                            rowId
                        );
                        expectedRowId++;
                    }
                }
            }
        }
    }

    /**
     * Verifies that the Lucene segment is physically sorted by the configured IndexSort
     * (age DESC nulls first). Reads age from sorted numeric doc values.
     */
    protected void verifyLuceneSortOrder() throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                org.apache.lucene.index.LeafReader leaf = ctx.reader();
                int maxDoc = leaf.maxDoc();
                if (maxDoc <= 1) continue;

                SortedNumericDocValues ageDV = leaf.getSortedNumericDocValues("age");

                Long prevAge = null;
                boolean prevAgeNull = true;
                for (int doc = 0; doc < maxDoc; doc++) {
                    Long age = null;
                    if (ageDV != null && ageDV.advanceExact(doc)) {
                        age = ageDV.nextValue();
                    }

                    if (doc > 0) {
                        // age DESC nulls first
                        if (prevAgeNull && age == null) {
                            // both null — ok
                        } else if (prevAgeNull) {
                            // prev was null, current is non-null — ok (nulls first)
                        } else if (age == null) {
                            fail("null age at doc " + doc + " came after non-null (expected nulls first)");
                        } else {
                            assertTrue("age should be DESC at doc " + doc + ", got " + prevAge + " before " + age, prevAge >= age);
                        }
                    }
                    prevAge = age;
                    prevAgeNull = (age == null);
                }
            }
        }
    }

    @SuppressForbidden(reason = "JSON parsing for row ID verification")
    protected void verifyParquetRowIdSequential(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        Path parquetDir = getParquetDir();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                String json = RustBridge.readAsJson(filePath.toString());
                List<Map<String, Object>> rows;
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    rows = parser.list().stream().map(o -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) o;
                        return m;
                    }).toList();
                }

                long expectedRowId = 0;
                for (Map<String, Object> row : rows) {
                    Object rowIdObj = row.get(DocumentInput.ROW_ID_FIELD);
                    assertNotNull(
                        DocumentInput.ROW_ID_FIELD + " should be present in parquet row " + expectedRowId + " in " + file,
                        rowIdObj
                    );
                    long rowId = ((Number) rowIdObj).longValue();
                    assertEquals(
                        DocumentInput.ROW_ID_FIELD
                            + " should be sequential, expected "
                            + expectedRowId
                            + " but got "
                            + rowId
                            + " in "
                            + file,
                        expectedRowId,
                        rowId
                    );
                    expectedRowId++;
                }
            }
        }
    }

    /**
     * Reads Parquet rows and Lucene documents in physical (storage) order from the
     * single-shard, single-segment index and asserts that at every position i,
     * Parquet row i and Lucene doc i agree on keyword fields ({@code name} and
     * {@code tag}).
     * <p>
     * The Lucene secondary writer only persists text/keyword fields (in the inverted
     * index) and {@code __row_id__} (as doc values). Numeric fields like {@code age}
     * are intentionally NOT stored in the Lucene secondary — they live only in
     * Parquet. Keyword fields are also stored only in the inverted index (no doc
     * values), so we read them via TermsEnum/postings and reconstruct the per-doc
     * value.
     * <p>
     * The {@code tag} field is NOT part of the sort key — verifying it confirms the
     * row ID rewrite correctly co-locates non-sort fields across formats too.
     */
    @SuppressForbidden(reason = "JSON parsing for cross-format row alignment")
    protected void verifyParquetAndLuceneRowsAlignedSequentially(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        // Read Parquet rows in physical order
        Path parquetDir = getParquetDir();
        List<Map<String, Object>> parquetRows = new ArrayList<>();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                String json = RustBridge.readAsJson(parquetDir.resolve(file).toString());
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    for (Object obj : parser.list()) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> row = (Map<String, Object>) obj;
                        parquetRows.add(row);
                    }
                }
            }
        }

        // Reconstruct per-doc keyword field values from Lucene's inverted index
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Test assumes a single Lucene leaf for sequential alignment", 1, reader.leaves().size());
            org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();

            String[] luceneNames = readKeywordValuesPerDoc(leaf, "name");
            String[] luceneTags = readKeywordValuesPerDoc(leaf, "tag");

            assertEquals("Parquet and Lucene must have same row count", parquetRows.size(), luceneNames.length);

            for (int i = 0; i < parquetRows.size(); i++) {
                Map<String, Object> pq = parquetRows.get(i);
                String pqName = (String) pq.get("name");
                String pqTag = (String) pq.get("tag");
                assertEquals("name mismatch at position " + i, pqName, luceneNames[i]);
                assertEquals("tag mismatch at position " + i, pqTag, luceneTags[i]);
            }
        }
    }

    /**
     * Asserts that the {@code name} keyword field, read in Lucene doc order from
     * the single-segment index, matches the given expected sequence. Used by
     * unsorted-path tests to confirm insertion order is preserved end-to-end.
     */
    protected void verifyLuceneNamesInOrder(String[] expectedNames) throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Test assumes a single Lucene leaf for sequential alignment", 1, reader.leaves().size());
            org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();
            String[] actual = readKeywordValuesPerDoc(leaf, "name");
            assertEquals("Lucene doc count should match expected sequence length", expectedNames.length, actual.length);
            for (int i = 0; i < expectedNames.length; i++) {
                assertEquals("name at lucene docId " + i, expectedNames[i], actual[i]);
            }
        }
    }

    /**
     * Reconstructs the per-doc keyword value for the given field by iterating
     * the inverted index. Each doc is expected to have at most one term for the
     * field. Returns an array indexed by Lucene doc ID.
     */
    protected String[] readKeywordValuesPerDoc(org.apache.lucene.index.LeafReader leaf, String fieldName) throws IOException {
        String[] values = new String[leaf.maxDoc()];
        org.apache.lucene.index.Terms terms = leaf.terms(fieldName);
        assertNotNull("Lucene index should have field '" + fieldName + "' in inverted index", terms);
        org.apache.lucene.index.TermsEnum it = terms.iterator();
        org.apache.lucene.util.BytesRef term;
        while ((term = it.next()) != null) {
            String value = term.utf8ToString();
            org.apache.lucene.index.PostingsEnum postings = it.postings(null);
            int doc;
            while ((doc = postings.nextDoc()) != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                values[doc] = value;
            }
        }
        return values;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: accessors
    // ══════════════════════════════════════════════════════════════════════

    protected DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, getNodeName());
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        IndexShard shard = indexService.getShard(0);
        CommitStats commitStats = shard.commitStats();
        assertNotNull(commitStats);
        assertNotNull(commitStats.getUserData());
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        return DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
    }

    protected Path getParquetDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().getDataPath().resolve("parquet");
    }

    protected Path getLuceneDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().resolveIndex();
    }

    protected IndexShard getPrimaryShard() {
        String nodeName = getNodeName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    protected String getNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return getClusterState().nodes().get(nodeId).getName();
    }
}
