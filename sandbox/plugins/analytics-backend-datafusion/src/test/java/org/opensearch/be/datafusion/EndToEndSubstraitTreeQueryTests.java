/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.IndexFilterCollectorProvider;
import org.opensearch.index.engine.exec.IndexFilterDelegate;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.arrow.c.Data.importField;

/**
 * End-to-end integration tests for the Substrait-driven tree query pipeline
 * with a real Lucene index providing bitsets and DataFusion scanning parquet.
 * <p>
 * Exercises the full cross-engine flow:
 * <ol>
 *   <li>Index documents into a real Lucene index (field {@code message})</li>
 *   <li>Use {@code test.parquet} for columnar data
 *       (2 rows: message=2,3; message2=3,4; message3=4,5)</li>
 *   <li>Register a real Lucene-backed {@link IndexFilterCollectorProvider}
 *       with {@link org.opensearch.index.engine.exec.IndexFilterBridge}</li>
 *   <li>Generate Substrait bytes with {@code index_filter('message','2')}
 *       via {@link NativeBridge#sqlToSubstraitWithIndexFilter}</li>
 *   <li>Execute via {@link NativeBridge#executeSubstraitTreeQueryAsync} —
 *       Rust extracts filter from Substrait → builds boolean tree →
 *       JNI callbacks to real Lucene Weight/Scorer → bitset intersection
 *       with parquet predicates (phase 2)</li>
 *   <li>Consume Arrow result stream and assert on actual values</li>
 * </ol>
 */
public class EndToEndSubstraitTreeQueryTests extends OpenSearchTestCase {

    private long runtimePtr;
    private long readerPtr;
    private Path parquetPath;
    private IndexWriter indexWriter;
    private MMapDirectory luceneDirectory;
    private DirectoryReader luceneReader;

    private static boolean runtimeInitialized = false;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (runtimeInitialized == false) {
            NativeBridge.initTokioRuntimeManager(2);
            runtimeInitialized = true;
        }
        Path spillDir = createTempDir("datafusion-spill");
        runtimePtr = NativeBridge.createGlobalRuntime(
            128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024
        );

        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        parquetPath = dataDir.resolve("test.parquet");
        Files.copy(testParquet, parquetPath);
        readerPtr = NativeBridge.createDatafusionReader(dataDir.toString(), new String[] { "test.parquet" });

        // Build a real Lucene index matching the parquet data.
        // test.parquet: row 0 → message=2, message2=3, message3=4
        //               row 1 → message=3, message2=4, message3=5
        Path luceneDir = createTempDir("lucene-index");
        luceneDirectory = new MMapDirectory(luceneDir);
        indexWriter = new IndexWriter(luceneDirectory, new IndexWriterConfig());

        Document doc0 = new Document();
        doc0.add(new StringField("message", "2", Field.Store.NO));
        indexWriter.addDocument(doc0);

        Document doc1 = new Document();
        doc1.add(new StringField("message", "3", Field.Store.NO));
        indexWriter.addDocument(doc1);

        indexWriter.commit();
        luceneReader = DirectoryReader.open(indexWriter);
    }

    @Override
    public void tearDown() throws Exception {
        NativeBridge.closeDatafusionReader(readerPtr);
        NativeBridge.closeGlobalRuntime(runtimePtr);
        luceneReader.close();
        indexWriter.close();
        luceneDirectory.close();
        super.tearDown();
    }

    /**
     * {@code SELECT message, message2 FROM test_table
     *        WHERE index_filter('message', '2') AND message2 > 2}
     * <p>
     * Lucene matches doc 0 (message="2"). Predicate message2 > 2 true for both rows.
     * Expected: 1 row → message=2, message2=3.
     */
    public void testLuceneIndexFilterWithParquetPredicate() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            List<Object[]> rows = executeQuery(delegate.getContextId(),
                "SELECT message, message2 FROM test_table WHERE index_filter('message', '2') AND message2 > 2");
            assertEquals("Expected 1 row", 1, rows.size());
            assertEquals(2L, rows.get(0)[0]);
            assertEquals(3L, rows.get(0)[1]);
        }
    }

    /**
     * {@code SELECT message, message2 FROM test_table WHERE index_filter('message', '3')}
     * Expected: 1 row → message=3, message2=4.
     */
    public void testLuceneIndexFilterOnly() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            long contextId = delegate.getContextId();

            // Test through the full JNI pipeline
            List<Object[]> rows = executeQuery(contextId,
                "SELECT message, message2 FROM test_table WHERE index_filter('message', '3')");
            assertEquals(1, rows.size());
            assertEquals(3L, rows.get(0)[0]);
            assertEquals(4L, rows.get(0)[1]);
        }
    }

    /**
     * {@code SELECT message, message2 FROM test_table WHERE index_filter('message', '999')}
     * No matches. Expected: 0 rows.
     */
    public void testLuceneIndexFilterNoMatches() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            List<Object[]> rows = executeQuery(delegate.getContextId(),
                "SELECT message, message2 FROM test_table WHERE index_filter('message', '999')");
            assertEquals(0, rows.size());
        }
    }

    /**
     * Verifies the inline Lucene collector works correctly in isolation
     * before testing through the full JNI pipeline.
     */
    public void testInlineLuceneCollectorDirectly() throws Exception {
        InlineLuceneCollectorProvider provider = new InlineLuceneCollectorProvider(luceneReader);

        // Create provider for "message:2"
        int pk = provider.createProvider("message:2".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertTrue("provider key should be positive", pk > 0);

        // Create collector for segment 0, doc range [0, 2)
        int ck = provider.createCollector(pk, 0, 0, 2);
        assertTrue("collector key should be positive", ck > 0);

        // Collect docs — should return bitset with doc 0 set
        long[] bitset = provider.collectDocs(ck, 0, 2);
        assertTrue("bitset should not be empty", bitset.length > 0);
        assertEquals("bit 0 should be set (doc 0 matches 'message:2')", 1L, bitset[0]);

        provider.releaseCollector(ck);
        provider.releaseProvider(pk);
        provider.close();
    }

    /**
     * {@code SELECT message, message2, message3 FROM test_table
     *        WHERE index_filter('message', '2') OR index_filter('message', '3')}
     * Both docs match. Expected: 2 rows.
     */
    public void testOrOfTwoLuceneIndexFilters() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            List<Object[]> rows = executeQuery(delegate.getContextId(),
                "SELECT message, message2, message3 FROM test_table "
                    + "WHERE index_filter('message', '2') OR index_filter('message', '3')");
            assertEquals(2, rows.size());
            rows.sort((a, b) -> Long.compare((Long) a[0], (Long) b[0]));
            assertEquals(2L, rows.get(0)[0]);
            assertEquals(3L, rows.get(0)[1]);
            assertEquals(4L, rows.get(0)[2]);
            assertEquals(3L, rows.get(1)[0]);
            assertEquals(4L, rows.get(1)[1]);
            assertEquals(5L, rows.get(1)[2]);
        }
    }

    // ── Test 5: Complex nested AND/OR/NOT with mixed index_filter and predicates ──

    /**
     * Complex boolean tree:
     * {@code SELECT message, message2, message3 FROM test_table
     *        WHERE (index_filter('message', '2') AND message2 > 2)
     *           OR (index_filter('message', '3') AND message3 < 6)}
     * <p>
     * Left branch: index_filter('message','2') matches doc 0, message2 &gt; 2 is true
     * for doc 0 (message2=3). Left branch → row 0.
     * Right branch: index_filter('message','3') matches doc 1, message3 &lt; 6 is true
     * for doc 1 (message3=5). Right branch → row 1.
     * OR of both → 2 rows.
     */
    public void testComplexNestedAndOrWithMixedFilters() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            List<Object[]> rows = executeQuery(delegate.getContextId(),
                "SELECT message, message2, message3 FROM test_table "
                    + "WHERE (index_filter('message', '2') AND message2 > 2) "
                    + "   OR (index_filter('message', '3') AND message3 < 6)");
            assertEquals("Expected 2 rows from OR of two AND branches", 2, rows.size());
            rows.sort((a, b) -> Long.compare((Long) a[0], (Long) b[0]));
            assertEquals(2L, rows.get(0)[0]);  // message=2
            assertEquals(3L, rows.get(0)[1]);  // message2=3
            assertEquals(4L, rows.get(0)[2]);  // message3=4
            assertEquals(3L, rows.get(1)[0]);  // message=3
            assertEquals(4L, rows.get(1)[1]);  // message2=4
            assertEquals(5L, rows.get(1)[2]);  // message3=5
        }
    }

    // ── Test 6: AND of index_filter with NOT of another index_filter ──

    /**
     * {@code SELECT message, message2 FROM test_table
     *        WHERE index_filter('message', '2') AND NOT index_filter('message', '3')}
     * <p>
     * index_filter('message','2') matches doc 0.
     * NOT index_filter('message','3') excludes doc 1 (matches doc 0).
     * AND → doc 0 only. Expected: 1 row → message=2, message2=3.
     */
    public void testAndWithNotIndexFilter() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            List<Object[]> rows = executeQuery(delegate.getContextId(),
                "SELECT message, message2 FROM test_table "
                    + "WHERE index_filter('message', '2') AND NOT index_filter('message', '3')");
            assertEquals("Expected 1 row (AND with NOT)", 1, rows.size());
            assertEquals(2L, rows.get(0)[0]);  // message=2
            assertEquals(3L, rows.get(0)[1]);  // message2=3
        }
    }

    // ── Test 7: Deeply nested: (A AND B) OR (C AND NOT D) with predicates ──

    /**
     * {@code SELECT message, message2, message3 FROM test_table
     *        WHERE (index_filter('message', '2') AND message3 > 3)
     *           OR (message2 < 5 AND NOT index_filter('message', '2'))}
     * <p>
     * Left: index_filter('message','2') → doc 0, message3 &gt; 3 → true (message3=4). → row 0.
     * Right: message2 &lt; 5 → both rows, NOT index_filter('message','2') → doc 1. → row 1.
     * OR → both rows.
     */
    public void testDeeplyNestedWithPredicatesAndNot() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            List<Object[]> rows = executeQuery(delegate.getContextId(),
                "SELECT message, message2, message3 FROM test_table "
                    + "WHERE (index_filter('message', '2') AND message3 > 3) "
                    + "   OR (message2 < 5 AND NOT index_filter('message', '2'))");
            assertEquals("Expected 2 rows from deeply nested OR", 2, rows.size());
            rows.sort((a, b) -> Long.compare((Long) a[0], (Long) b[0]));
            assertEquals(2L, rows.get(0)[0]);
            assertEquals(3L, rows.get(0)[1]);
            assertEquals(4L, rows.get(0)[2]);
            assertEquals(3L, rows.get(1)[0]);
            assertEquals(4L, rows.get(1)[1]);
            assertEquals(5L, rows.get(1)[2]);
        }
    }

    // ── Test 8: AND of index_filter + predicate that eliminates all rows ──

    /**
     * {@code SELECT message, message2 FROM test_table
     *        WHERE index_filter('message', '2') AND message2 > 100}
     * <p>
     * index_filter matches doc 0, but message2 &gt; 100 is false for all rows.
     * Expected: 0 rows.
     */
    public void testIndexFilterWithImpossiblePredicate() throws Exception {
        try (IndexFilterDelegate delegate = createDelegate()) {
            List<Object[]> rows = executeQuery(delegate.getContextId(),
                "SELECT message, message2 FROM test_table "
                    + "WHERE index_filter('message', '2') AND message2 > 100");
            assertEquals("Expected 0 rows (predicate eliminates all)", 0, rows.size());
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private IndexFilterDelegate createDelegate() {
        InlineLuceneCollectorProvider provider = new InlineLuceneCollectorProvider(luceneReader);
        DataFormat format = new TestDataFormat("lucene");
        Map<String, DataFormat> col2fmt = new HashMap<>();
        col2fmt.put("message", format);
        Map<DataFormat, IndexFilterCollectorProvider> providers = new HashMap<>();
        providers.put(format, provider);
        return new IndexFilterDelegate(col2fmt, providers);
    }

    private List<Object[]> executeQuery(long contextId, String sql) {
        byte[] substraitBytes = NativeBridge.sqlToSubstraitWithIndexFilter(readerPtr, "test_table", sql, runtimePtr);
        assertNotNull(substraitBytes);
        assertTrue(substraitBytes.length > 0);
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeSubstraitTreeQueryAsync(contextId, new String[] { parquetPath.toString() },
            "test_table", substraitBytes, 1, runtimePtr,
            new ActionListener<>() {
                @Override public void onResponse(Long v) { future.complete(v); }
                @Override public void onFailure(Exception e) { future.completeExceptionally(e); }
            });
        long streamPtr = future.join();
        assertTrue(streamPtr != 0);
        return consumeStream(streamPtr);
    }

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override public void onResponse(Long v) { f.complete(v); }
            @Override public void onFailure(Exception e) { f.completeExceptionally(e); }
        });
        return f.join();
    }

    private List<Object[]> consumeStream(long streamPtr) {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             CDataDictionaryProvider dict = new CDataDictionaryProvider()) {
            long schemaAddr = asyncCall(l -> NativeBridge.streamGetSchema(streamPtr, l));
            Schema schema = new Schema(importField(alloc, ArrowSchema.wrap(schemaAddr), dict).getChildren(), null);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
            List<Object[]> rows = new ArrayList<>();
            while (true) {
                long arrayAddr = asyncCall(l -> NativeBridge.streamNext(runtimePtr, streamPtr, l));
                if (arrayAddr == 0) break;
                Data.importIntoVectorSchemaRoot(alloc, ArrowArray.wrap(arrayAddr), root, dict);
                for (int r = 0; r < root.getRowCount(); r++) {
                    Object[] row = new Object[root.getFieldVectors().size()];
                    for (int c = 0; c < row.length; c++) row[c] = root.getFieldVectors().get(c).getObject(r);
                    rows.add(row);
                }
            }
            root.close();
            NativeBridge.streamClose(streamPtr);
            return rows;
        }
    }

    // ── Inline Lucene-backed IndexFilterCollectorProvider ────────────

    private static class InlineLuceneCollectorProvider implements IndexFilterCollectorProvider {
        private final DirectoryReader reader;
        private final ConcurrentHashMap<Integer, WeightEntry> weights = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, CollectorEntry> collectors = new ConcurrentHashMap<>();
        private final AtomicInteger nextPK = new AtomicInteger(1);
        private final AtomicInteger nextCK = new AtomicInteger(1);

        private record WeightEntry(Weight weight, List<LeafReaderContext> leaves) {}
        private record CollectorEntry(Scorer scorer, int minDoc, int maxDoc, int pk) {}

        InlineLuceneCollectorProvider(DirectoryReader reader) { this.reader = reader; }

        @Override public int createProvider(byte[] queryBytes) throws IOException {
            String q = new String(queryBytes, StandardCharsets.UTF_8);
            System.err.println("[InlineLucene] createProvider: query=" + q);
            int colon = q.indexOf(':');
            Query query = new TermQuery(new Term(q.substring(0, colon), q.substring(colon + 1)));
            IndexSearcher s = new IndexSearcher(reader);
            Weight w = s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            int k = nextPK.getAndIncrement();
            weights.put(k, new WeightEntry(w, reader.leaves()));
            System.err.println("[InlineLucene] createProvider: key=" + k + " leaves=" + reader.leaves().size());
            return k;
        }

        @Override public int createCollector(int pk, int segOrd, int min, int max) {
            System.err.println("[InlineLucene] createCollector: pk=" + pk + " segOrd=" + segOrd + " min=" + min + " max=" + max);
            WeightEntry e = weights.get(pk);
            if (e == null) { System.err.println("[InlineLucene] createCollector: weight not found for pk=" + pk); return -1; }
            try {
                Scorer sc = e.weight.scorer(e.leaves.get(segOrd));
                int k = nextCK.getAndIncrement();
                collectors.put(k, new CollectorEntry(sc, min, max, pk));
                System.err.println("[InlineLucene] createCollector: key=" + k + " scorer=" + (sc != null ? "present" : "null"));
                return k;
            } catch (IOException ex) { System.err.println("[InlineLucene] createCollector: IOException: " + ex); return -1; }
        }

        @Override public long[] collectDocs(int ck, int min, int max) {
            CollectorEntry e = collectors.get(ck);
            if (e == null || e.scorer == null) {
                System.err.println("[InlineLucene] collectDocs: EARLY RETURN - e=" + (e == null ? "null" : "scorer=null")
                    + " ck=" + ck + " collectors.keys=" + collectors.keySet());
                return new long[0];
            }
            int lo = Math.max(min, e.minDoc), hi = Math.min(max, e.maxDoc);
            if (lo >= hi) {
                System.err.println("[InlineLucene] collectDocs: EARLY RETURN - lo(" + lo + ") >= hi(" + hi + ")");
                return new long[0];
            }
            BitSet bs = new BitSet(hi - lo);
            try {
                DocIdSetIterator it = e.scorer.iterator();
                int d = it.advance(lo);
                while (d != DocIdSetIterator.NO_MORE_DOCS && d < hi) { bs.set(d - lo); d = it.nextDoc(); }
            } catch (Exception ex) {
                System.err.println("[InlineLucene] collectDocs: EXCEPTION: " + ex.getClass().getName() + ": " + ex.getMessage());
                ex.printStackTrace(System.err);
                return new long[0];
            }
            long[] result = bs.toLongArray();
            System.err.println("[InlineLucene] collectDocs(ck=" + ck + ", min=" + min + ", max=" + max
                + ") → lo=" + lo + " hi=" + hi + " bitset=" + java.util.Arrays.toString(result)
                + " cardinality=" + bs.cardinality());
            return result;
        }

        @Override public void releaseCollector(int ck) { collectors.remove(ck); }
        @Override public void releaseProvider(int pk) { weights.remove(pk); collectors.entrySet().removeIf(e -> e.getValue().pk == pk); }
        @Override public void close() { collectors.clear(); weights.clear(); }
    }

    private static class TestDataFormat extends DataFormat {
        private final String name;
        TestDataFormat(String name) { this.name = name; }
        @Override public String name() { return name; }
        @Override public long priority() { return 100L; }
        @Override public Set<FieldTypeCapabilities> supportedFields() { return Set.of(); }
    }
}
