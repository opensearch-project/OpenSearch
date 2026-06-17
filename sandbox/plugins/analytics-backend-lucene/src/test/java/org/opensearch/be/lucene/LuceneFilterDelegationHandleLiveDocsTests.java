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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for liveDocs filtering in {@link LuceneFilterDelegationHandle}.
 *
 * Validates three scenarios:
 * 1. Parquet-only format with deletions: synthetic MATCHALL delegation filters deleted docs
 *    via liveDocs in collectDocs().
 * 2. Parquet + Lucene (composite): real delegated predicate (e.g., TermQuery) combined with
 *    liveDocs filtering — deleted docs that match the predicate are excluded.
 * 3. Lucene-only: delegation handle correctly reports liveDocs=null (no deletions) — all
 *    matching docs pass through.
 */
public class LuceneFilterDelegationHandleLiveDocsTests extends OpenSearchTestCase {

    private static final String LUCENE_BACKEND = "lucene";

    private Directory directory;
    private IndexWriter writer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        // Disable merges so that deleted docs remain as liveDocs bitset
        // rather than being physically removed by segment merging.
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        writer = new IndexWriter(directory, config);
    }

    @Override
    public void tearDown() throws Exception {
        writer.close();
        directory.close();
        super.tearDown();
    }

    /**
     * Parquet-only with deletions: synthetic MATCHALL delegation.
     * All live docs should pass; deleted docs should be excluded.
     *
     * Simulates: index 10 docs, delete 3, use MATCHALL delegation → expect 7 bits set.
     */
    public void testParquetOnly_MatchAllDelegation_ExcludesDeletedDocs() throws Exception {
        // Index 10 docs
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc" + i, Field.Store.NO));
            doc.add(new StringField("status", String.valueOf(i % 3), Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();

        // Delete docs 2, 5, 8 (every third doc starting at 2)
        writer.deleteDocuments(new Term("_id", "doc2"));
        writer.deleteDocuments(new Term("_id", "doc5"));
        writer.deleteDocuments(new Term("_id", "doc8"));
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        try {
            LuceneFilterDelegationHandle handle = createHandle(reader, serializeQuery(new MatchAllQueryBuilder()));

            LeafReaderContext leaf = reader.leaves().get(0);
            assertNotNull("Segment must have liveDocs after deletions", leaf.reader().getLiveDocs());

            int providerKey = handle.createProvider(0);
            assertTrue("Provider key must be valid", providerKey >= 0);

            int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf.reader().maxDoc());
            assertTrue("Collector key must be valid", collectorKey >= 0);

            long[] bitset = collectBitset(handle, collectorKey, 0, leaf.reader().maxDoc());

            int liveCount = countBits(bitset, leaf.reader().maxDoc());
            assertEquals("Should have 7 live docs (10 total - 3 deleted)", 7, liveCount);

            // Verify specific deleted positions are NOT set
            assertFalse("Doc 2 should be excluded (deleted)", isBitSet(bitset, 2));
            assertFalse("Doc 5 should be excluded (deleted)", isBitSet(bitset, 5));
            assertFalse("Doc 8 should be excluded (deleted)", isBitSet(bitset, 8));

            // Verify some live positions ARE set
            assertTrue("Doc 0 should be included (live)", isBitSet(bitset, 0));
            assertTrue("Doc 1 should be included (live)", isBitSet(bitset, 1));
            assertTrue("Doc 3 should be included (live)", isBitSet(bitset, 3));

            handle.releaseCollector(collectorKey);
            handle.releaseProvider(providerKey);
            handle.close();
        } finally {
            reader.close();
        }
    }

    /**
     * Parquet + Lucene composite: real TermQuery delegation with deletions.
     * Only live docs that ALSO match the delegated predicate should pass.
     *
     * Simulates: index 10 docs with status "match" or "nomatch", delete some matching docs,
     * delegate a TermQuery for status=match → only live matching docs in bitset.
     */
    public void testParquetPlusLucene_TermQueryDelegation_ExcludesDeletedMatchingDocs() throws Exception {
        // Index 10 docs: even-indexed have status=match, odd have status=nomatch
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc" + i, Field.Store.NO));
            doc.add(new StringField("status", i % 2 == 0 ? "match" : "nomatch", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();

        // Delete docs 0 and 4 (both have status=match)
        writer.deleteDocuments(new Term("_id", "doc0"));
        writer.deleteDocuments(new Term("_id", "doc4"));
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        try {
            // Use a raw Lucene TermQuery directly via a custom handle (bypass QueryBuilder serialization)
            LuceneFilterDelegationHandle handle = createHandleWithRawQuery(
                reader,
                new org.apache.lucene.search.TermQuery(new Term("status", "match"))
            );

            LeafReaderContext leaf = reader.leaves().get(0);
            assertNotNull("Segment must have liveDocs after deletions", leaf.reader().getLiveDocs());

            int providerKey = handle.createProvider(0);
            assertTrue("Provider key must be valid", providerKey >= 0);

            int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf.reader().maxDoc());
            assertTrue("Collector key must be valid", collectorKey >= 0);

            long[] bitset = collectBitset(handle, collectorKey, 0, leaf.reader().maxDoc());

            int matchCount = countBits(bitset, leaf.reader().maxDoc());
            // Originally 5 docs match (0,2,4,6,8), minus 2 deleted (0,4) = 3 live matches
            assertEquals("Should have 3 live matching docs", 3, matchCount);

            // Deleted matching docs should NOT be in bitset
            assertFalse("Doc 0 should be excluded (deleted)", isBitSet(bitset, 0));
            assertFalse("Doc 4 should be excluded (deleted)", isBitSet(bitset, 4));

            // Live matching docs should be in bitset
            assertTrue("Doc 2 should be included (live, matches)", isBitSet(bitset, 2));
            assertTrue("Doc 6 should be included (live, matches)", isBitSet(bitset, 6));
            assertTrue("Doc 8 should be included (live, matches)", isBitSet(bitset, 8));

            // Non-matching docs should NOT be in bitset regardless of liveness
            assertFalse("Doc 1 should be excluded (doesn't match)", isBitSet(bitset, 1));
            assertFalse("Doc 3 should be excluded (doesn't match)", isBitSet(bitset, 3));

            handle.releaseCollector(collectorKey);
            handle.releaseProvider(providerKey);
            handle.close();
        } finally {
            reader.close();
        }
    }

    /**
     * Lucene-only (no deletions): liveDocs is null, all matching docs pass.
     * Verifies that collectDocs works correctly when there are no deleted docs.
     */
    public void testLuceneOnly_NoDeletions_AllMatchingDocsPass() throws Exception {
        // Index 10 docs, NO deletions
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc" + i, Field.Store.NO));
            doc.add(new StringField("status", i % 2 == 0 ? "match" : "nomatch", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        try {
            LuceneFilterDelegationHandle handle = createHandleWithRawQuery(
                reader,
                new org.apache.lucene.search.TermQuery(new Term("status", "match"))
            );

            LeafReaderContext leaf = reader.leaves().get(0);
            assertNull("liveDocs should be null when no deletions exist", leaf.reader().getLiveDocs());

            int providerKey = handle.createProvider(0);
            assertTrue("Provider key must be valid", providerKey >= 0);

            int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf.reader().maxDoc());
            assertTrue("Collector key must be valid", collectorKey >= 0);

            long[] bitset = collectBitset(handle, collectorKey, 0, leaf.reader().maxDoc());

            int matchCount = countBits(bitset, leaf.reader().maxDoc());
            // 5 even-indexed docs match status=match, no deletions
            assertEquals("Should have 5 matching docs (no deletions)", 5, matchCount);

            // All even-indexed docs should be set
            assertTrue("Doc 0 should match", isBitSet(bitset, 0));
            assertTrue("Doc 2 should match", isBitSet(bitset, 2));
            assertTrue("Doc 4 should match", isBitSet(bitset, 4));
            assertTrue("Doc 6 should match", isBitSet(bitset, 6));
            assertTrue("Doc 8 should match", isBitSet(bitset, 8));

            handle.releaseCollector(collectorKey);
            handle.releaseProvider(providerKey);
            handle.close();
        } finally {
            reader.close();
        }
    }

    /**
     * MATCHALL with no deletions: all docs should be in the bitset.
     * Baseline sanity check.
     */
    public void testMatchAll_NoDeletions_AllDocsPass() throws Exception {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc" + i, Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        try {
            LuceneFilterDelegationHandle handle = createHandle(reader, serializeQuery(new MatchAllQueryBuilder()));

            LeafReaderContext leaf = reader.leaves().get(0);
            assertNull("liveDocs should be null when no deletions", leaf.reader().getLiveDocs());

            int providerKey = handle.createProvider(0);
            int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf.reader().maxDoc());

            long[] bitset = collectBitset(handle, collectorKey, 0, leaf.reader().maxDoc());

            int count = countBits(bitset, leaf.reader().maxDoc());
            assertEquals("All 10 docs should pass", 10, count);

            handle.releaseCollector(collectorKey);
            handle.releaseProvider(providerKey);
            handle.close();
        } finally {
            reader.close();
        }
    }

    /**
     * All docs deleted: MATCHALL delegation returns empty bitset.
     */
    public void testAllDocsDeleted_EmptyBitset() throws Exception {
        for (int i = 0; i < 5; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc" + i, Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();

        // Delete all
        for (int i = 0; i < 5; i++) {
            writer.deleteDocuments(new Term("_id", "doc" + i));
        }
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        try {
            // After deleting all docs, the segment may be removed entirely
            if (reader.leaves().isEmpty()) {
                // Valid case: Lucene can drop segments with no live docs
                reader.close();
                return;
            }

            LuceneFilterDelegationHandle handle = createHandle(reader, serializeQuery(new MatchAllQueryBuilder()));

            LeafReaderContext leaf = reader.leaves().get(0);
            int providerKey = handle.createProvider(0);
            int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf.reader().maxDoc());

            long[] bitset = collectBitset(handle, collectorKey, 0, leaf.reader().maxDoc());

            int count = countBits(bitset, leaf.reader().maxDoc());
            assertEquals("No live docs — bitset should be empty", 0, count);

            handle.releaseCollector(collectorKey);
            handle.releaseProvider(providerKey);
            handle.close();
        } finally {
            reader.close();
        }
    }

    /**
     * Update scenario (delete + re-index same ID): only the new version should appear.
     * Simulates what happens on an update: the old doc is soft-deleted, a new doc is added.
     */
    public void testUpdate_OldVersionExcluded_NewVersionIncluded() throws Exception {
        // Index original doc
        Document original = new Document();
        original.add(new StringField("_id", "doc1", Field.Store.NO));
        original.add(new StringField("version", "v1", Field.Store.NO));
        writer.addDocument(original);
        writer.commit();

        // Update: delete old, add new (in same segment after commit)
        writer.deleteDocuments(new Term("_id", "doc1"));
        Document updated = new Document();
        updated.add(new StringField("_id", "doc1", Field.Store.NO));
        updated.add(new StringField("version", "v2", Field.Store.NO));
        writer.addDocument(updated);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        try {
            LuceneFilterDelegationHandle handle = createHandle(reader, serializeQuery(new MatchAllQueryBuilder()));

            // There may be 1 or 2 segments depending on merge behavior
            int totalLive = 0;
            for (LeafReaderContext leaf : reader.leaves()) {
                String segName = ((SegmentReader) leaf.reader()).getSegmentInfo().info.name;
                long gen = getGenerationForSegment(reader, segName);

                int providerKey = handle.createProvider(0);
                int collectorKey = handle.createCollector(providerKey, gen, 0, leaf.reader().maxDoc());
                if (collectorKey < 0) {
                    handle.releaseProvider(providerKey);
                    continue;
                }

                long[] bitset = collectBitset(handle, collectorKey, 0, leaf.reader().maxDoc());
                totalLive += countBits(bitset, leaf.reader().maxDoc());

                handle.releaseCollector(collectorKey);
                handle.releaseProvider(providerKey);
            }

            // Only the new version should be live
            assertEquals("Should have exactly 1 live doc (the updated version)", 1, totalLive);

            handle.close();
        } finally {
            reader.close();
        }
    }

    /**
     * Partial range collectDocs: validates that liveDocs filtering works correctly
     * when collecting a sub-range [minDoc, maxDoc) within a segment.
     */
    public void testPartialRange_LiveDocsFilteringCorrect() throws Exception {
        // Index 20 docs
        for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", "doc" + i, Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();

        // Delete docs 5, 10, 15
        writer.deleteDocuments(new Term("_id", "doc5"));
        writer.deleteDocuments(new Term("_id", "doc10"));
        writer.deleteDocuments(new Term("_id", "doc15"));
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        try {
            LuceneFilterDelegationHandle handle = createHandle(reader, serializeQuery(new MatchAllQueryBuilder()));

            LeafReaderContext leaf = reader.leaves().get(0);
            int providerKey = handle.createProvider(0);

            // Collect only range [4, 12) — should include docs 4,6,7,8,9,11 (exclude 5,10)
            int collectorKey = handle.createCollector(providerKey, 1L, 4, 12);
            assertTrue("Collector key must be valid", collectorKey >= 0);

            long[] bitset = collectBitset(handle, collectorKey, 4, 12);

            int count = countBits(bitset, 8); // range span = 12 - 4 = 8
            // Docs in range [4,12): 4,5,6,7,8,9,10,11 → minus deleted 5,10 = 6 live
            assertEquals("Should have 6 live docs in range [4,12)", 6, count);

            // Doc at offset 1 in the range (absolute doc 5) should be excluded
            assertFalse("Doc 5 (offset 1) should be excluded (deleted)", isBitSet(bitset, 1));
            // Doc at offset 6 in the range (absolute doc 10) should be excluded
            assertFalse("Doc 10 (offset 6) should be excluded (deleted)", isBitSet(bitset, 6));
            // Doc at offset 0 (absolute doc 4) should be included
            assertTrue("Doc 4 (offset 0) should be included (live)", isBitSet(bitset, 0));

            handle.releaseCollector(collectorKey);
            handle.releaseProvider(providerKey);
            handle.close();
        } finally {
            reader.close();
        }
    }

    // ─── Helper Methods ──────────────────────────────────────────────────────────

    private LuceneFilterDelegationHandle createHandle(DirectoryReader reader, byte[] queryBytes) {
        Map<Long, String> genMap = buildGenerationMap(reader);
        LuceneReader luceneReader = new LuceneReader(reader, genMap);
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryShardContext qsc = mockQueryShardContext(searcher);
        CatalogSnapshot catalogSnapshot = mock(CatalogSnapshot.class);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
            )
        );
        DelegatedExpression expr = new DelegatedExpression(0, LUCENE_BACKEND, queryBytes);
        return new LuceneFilterDelegationHandle(List.of(expr), qsc, luceneReader, catalogSnapshot, registry, () -> false);
    }

    /**
     * Creates a delegation handle that uses a raw Lucene Query directly,
     * bypassing QueryBuilder serialization/deserialization. Useful when testing
     * liveDocs filtering behavior without depending on QueryBuilder mocks.
     */
    private LuceneFilterDelegationHandle createHandleWithRawQuery(DirectoryReader reader, org.apache.lucene.search.Query rawQuery) {
        Map<Long, String> genMap = buildGenerationMap(reader);
        LuceneReader luceneReader = new LuceneReader(reader, genMap);
        IndexSearcher searcher = new IndexSearcher(reader);
        // Build a QueryShardContext whose searcher is wired and whose fieldMapper
        // returns a MappedFieldType that produces the raw query when termQuery is called.
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.searcher()).thenReturn(searcher);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.termQuery(any(), any())).thenReturn(rawQuery);
        when(qsc.fieldMapper(any())).thenReturn(fieldType);

        CatalogSnapshot catalogSnapshot = mock(CatalogSnapshot.class);
        // Serialize a TermQueryBuilder with a dummy field — the mock intercepts toQuery
        // and returns the raw query regardless of field name.
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
            )
        );
        byte[] queryBytes;
        try {
            queryBytes = serializeQuery(new TermQueryBuilder("_dummy", "_dummy"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DelegatedExpression expr = new DelegatedExpression(0, LUCENE_BACKEND, queryBytes);
        return new LuceneFilterDelegationHandle(List.of(expr), qsc, luceneReader, catalogSnapshot, registry, () -> false);
    }

    private Map<Long, String> buildGenerationMap(DirectoryReader reader) {
        Map<Long, String> genMap = new HashMap<>();
        long gen = 1L;
        for (LeafReaderContext leaf : reader.leaves()) {
            SegmentReader sr = (SegmentReader) leaf.reader();
            genMap.put(gen, sr.getSegmentInfo().info.name);
            gen++;
        }
        return genMap;
    }

    private long getGenerationForSegment(DirectoryReader reader, String segName) {
        Map<Long, String> genMap = buildGenerationMap(reader);
        for (Map.Entry<Long, String> entry : genMap.entrySet()) {
            if (entry.getValue().equals(segName)) {
                return entry.getKey();
            }
        }
        return -1;
    }

    private QueryShardContext mockQueryShardContext(IndexSearcher searcher) {
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.searcher()).thenReturn(searcher);
        MappedFieldType statusFieldType = mock(MappedFieldType.class);
        when(statusFieldType.termQuery(any(), any())).thenAnswer(invocation -> {
            Object value = invocation.getArgument(0);
            return new org.apache.lucene.search.TermQuery(new Term("status", value.toString()));
        });
        when(qsc.fieldMapper(eq("status"))).thenReturn(statusFieldType);

        MappedFieldType idFieldType = mock(MappedFieldType.class);
        when(idFieldType.termQuery(any(), any())).thenAnswer(invocation -> {
            Object value = invocation.getArgument(0);
            return new org.apache.lucene.search.TermQuery(new Term("_id", value.toString()));
        });
        when(qsc.fieldMapper(eq("_id"))).thenReturn(idFieldType);
        return qsc;
    }

    private byte[] serializeQuery(QueryBuilder qb) throws IOException {
        org.opensearch.common.io.stream.BytesStreamOutput out = new org.opensearch.common.io.stream.BytesStreamOutput();
        out.writeNamedWriteable(qb);
        return org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes());
    }

    private long[] collectBitset(FilterDelegationHandle handle, int collectorKey, int minDoc, int maxDoc) {
        int span = maxDoc - minDoc;
        int wordCount = (span + 63) >>> 6;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate((long) wordCount * Long.BYTES);
            int written = handle.collectDocs(collectorKey, minDoc, maxDoc, out);
            assertTrue("collectDocs should return non-negative word count", written >= 0);
            long[] words = new long[wordCount];
            for (int i = 0; i < written; i++) {
                words[i] = out.getAtIndex(ValueLayout.JAVA_LONG, i);
            }
            return words;
        }
    }

    private int countBits(long[] words, int maxBits) {
        int count = 0;
        for (int i = 0; i < maxBits; i++) {
            if (isBitSet(words, i)) count++;
        }
        return count;
    }

    private boolean isBitSet(long[] words, int bit) {
        int wordIndex = bit >>> 6;
        int bitIndex = bit & 63;
        if (wordIndex >= words.length) return false;
        return (words[wordIndex] & (1L << bitIndex)) != 0;
    }
}
