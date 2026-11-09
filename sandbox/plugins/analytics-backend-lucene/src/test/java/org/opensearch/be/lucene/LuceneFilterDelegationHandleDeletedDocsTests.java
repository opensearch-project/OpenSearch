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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LiveDocs;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntPredicate;

import static org.opensearch.analytics.spi.FilterDelegationHandle.LIVE_DOCS_MATCH_ALL_ANNOTATION_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link LuceneFilterDelegationHandle}'s deleted-doc filtering through its public API
 * (createProvider → createCollector → collectDocs) over a real index with deletions:
 *
 * <ul>
 *   <li>the reserved match-all provider's live-docs fast path ({@code fillLiveDocsWords}) for dense,
 *       sparse, and no-deletion segments, and
 *   <li>a delegated predicate's scorer path — the {@code scorer ∩ liveDocs} intersection (dense) and the
 *       raw scorer with per-doc {@code liveDocs.get} (sparse) — asserting deleted docs are excluded.
 * </ul>
 *
 * <p>Deletion density is chosen to force Lucene's live-docs representation: {@code SPARSE_DENSE_THRESHOLD}
 * is 1%, so &gt;1% deleted yields a {@code FixedBitSet}-backed dense representation (recoverable via
 * {@link BitSetIterator#getFixedBitSetOrNull}) and ≤1% yields a sparse one.
 */
public class LuceneFilterDelegationHandleDeletedDocsTests extends OpenSearchTestCase {

    private static final String PREDICATE_TAG = "hello";
    private static final int PREDICATE_ANNOTATION_ID = 1;

    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;

    @Override
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
        super.tearDown();
    }

    /** Index {@code numDocs} docs (field {@code tag} = hello for even ids, goodbye otherwise), then delete by id. */
    private void buildIndex(int numDocs, IntPredicate deleteIf) throws IOException {
        directory = new ByteBuffersDirectory();
        // NoMergePolicy keeps a single segment and prevents a merge from dropping deleted docs, so the
        // reopened reader exposes their liveDocs.
        writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
            doc.add(new StringField("tag", i % 2 == 0 ? PREDICATE_TAG : "goodbye", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();
        for (int i = 0; i < numDocs; i++) {
            if (deleteIf.test(i)) {
                writer.deleteDocuments(new Term("id", Integer.toString(i)));
            }
        }
        writer.commit();
        // Open from the committed directory (not the writer) so liveDocs are read back through the
        // codec's live-docs format, producing the dense/sparse LiveDocs the handle branches on.
        reader = DirectoryReader.open(directory);
        assertEquals("test assumes a single segment", 1, reader.leaves().size());
    }

    private LeafReader leaf() {
        return reader.leaves().get(0).reader();
    }

    private LuceneFilterDelegationHandle newHandle(List<DelegatedExpression> expressions) {
        String segName = ((SegmentReader) leaf()).getSegmentInfo().info.name;
        LuceneReader luceneReader = new LuceneReader(reader, Map.of(1L, segName));
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
        );
        return new LuceneFilterDelegationHandle(
            expressions,
            mockQueryShardContext(),
            luceneReader,
            mock(CatalogSnapshot.class),
            registry,
            () -> false
        );
    }

    /** Resolves field {@code tag} to a TermQuery, mirroring how a delegated TermQueryBuilder compiles. */
    private static QueryShardContext mockQueryShardContext() {
        QueryShardContext qsc = mock(QueryShardContext.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.termQuery(any(), any())).thenAnswer(invocation -> {
            // The deserialized value arrives as a BytesRef; use it as the term bytes directly rather
            // than via toString() (which yields the hex form and would never match the indexed term).
            Object value = invocation.getArgument(0);
            BytesRef term = value instanceof BytesRef ref ? ref : new BytesRef(value.toString());
            return new TermQuery(new Term("tag", term));
        });
        when(qsc.fieldMapper("tag")).thenReturn(fieldType);
        return qsc;
    }

    private static DelegatedExpression termExpression(int annotationId, String field, String value) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(new TermQueryBuilder(field, value));
            return new DelegatedExpression(annotationId, "lucene", BytesReference.toBytes(out.bytes()));
        }
    }

    /** Docs the segment considers live (all docs when there are no deletions). */
    private Set<Integer> liveDocIds() {
        Set<Integer> live = new TreeSet<>();
        Bits liveDocs = leaf().getLiveDocs();
        for (int i = 0; i < leaf().maxDoc(); i++) {
            if (liveDocs == null || liveDocs.get(i)) {
                live.add(i);
            }
        }
        return live;
    }

    /** Run collectDocs for {@code [minDoc, maxDoc)} and return the absolute doc ids it set. */
    private Set<Integer> collectRange(LuceneFilterDelegationHandle handle, int collectorKey, int minDoc, int maxDoc) {
        int span = maxDoc - minDoc;
        int wordCount = (span + 63) >>> 6;
        Set<Integer> collected = new TreeSet<>();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate((long) wordCount * Long.BYTES);
            long ret = handle.collectDocs(collectorKey, minDoc, maxDoc, out);
            assertTrue("collectDocs signalled an error", ret != -1);
            for (int rel = 0; rel < span; rel++) {
                long word = out.getAtIndex(ValueLayout.JAVA_LONG, rel >>> 6);
                if ((word & (1L << (rel & 63))) != 0) {
                    collected.add(minDoc + rel);
                }
            }
        }
        return collected;
    }

    /** createProvider + createCollector over the whole leaf, then collectDocs. */
    private Set<Integer> collectAll(LuceneFilterDelegationHandle handle, int annotationId) {
        int providerKey = handle.createProvider(annotationId);
        assertTrue("createProvider failed", providerKey > 0);
        int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf().maxDoc());
        assertTrue("createCollector failed", collectorKey > 0);
        return collectRange(handle, collectorKey, 0, leaf().maxDoc());
    }

    private void assertDenseLiveDocs() {
        Bits liveDocs = leaf().getLiveDocs();
        assertTrue("expected a LiveDocs representation", liveDocs instanceof LiveDocs);
        FixedBitSet fixed = BitSetIterator.getFixedBitSetOrNull(((LiveDocs) liveDocs).liveDocsIterator());
        assertNotNull("dense deletions must expose a FixedBitSet-backed live set", fixed);
    }

    private void assertSparseLiveDocs() {
        Bits liveDocs = leaf().getLiveDocs();
        assertTrue("expected a LiveDocs representation", liveDocs instanceof LiveDocs);
        FixedBitSet fixed = BitSetIterator.getFixedBitSetOrNull(((LiveDocs) liveDocs).liveDocsIterator());
        assertNull("sparse deletions must not expose a FixedBitSet-backed live set", fixed);
    }

    // ── match-all provider: fillLiveDocsWords fast path ──

    public void testMatchAllEmitsLiveDocsForDenseDeletions() throws Exception {
        buildIndex(1000, i -> i % 4 == 0); // 25% deleted → dense
        assertDenseLiveDocs();
        LuceneFilterDelegationHandle handle = newHandle(List.of());
        assertEquals(liveDocIds(), collectAll(handle, LIVE_DOCS_MATCH_ALL_ANNOTATION_ID));
    }

    public void testMatchAllEmitsLiveDocsForSparseDeletions() throws Exception {
        buildIndex(1000, i -> i == 3 || i == 250 || i == 777); // 0.3% deleted → sparse
        assertSparseLiveDocs();
        LuceneFilterDelegationHandle handle = newHandle(List.of());
        assertEquals(liveDocIds(), collectAll(handle, LIVE_DOCS_MATCH_ALL_ANNOTATION_ID));
    }

    public void testMatchAllEmitsAllDocsWithoutDeletions() throws Exception {
        buildIndex(300, i -> false);
        assertNull("no deletions should leave liveDocs null", leaf().getLiveDocs());
        LuceneFilterDelegationHandle handle = newHandle(List.of());
        Set<Integer> all = new TreeSet<>();
        for (int i = 0; i < 300; i++) {
            all.add(i);
        }
        assertEquals(all, collectAll(handle, LIVE_DOCS_MATCH_ALL_ANNOTATION_ID));
    }

    public void testMatchAllCopyLiveWordsHandlesUnalignedOffset() throws Exception {
        buildIndex(1000, i -> i % 4 == 0); // dense → copyLiveWords word-copy path
        assertDenseLiveDocs();
        LuceneFilterDelegationHandle handle = newHandle(List.of());
        int providerKey = handle.createProvider(LIVE_DOCS_MATCH_ALL_ANNOTATION_ID);
        int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf().maxDoc());

        // A sub-range starting mid-word (70 & 63 == 6) forces copyLiveWords' bit-shift branch, not the
        // aligned fast copy — asserts the shift + trailing mask are correct.
        int from = 70;
        Set<Integer> expected = new TreeSet<>();
        for (int i : liveDocIds()) {
            if (i >= from) {
                expected.add(i);
            }
        }
        assertEquals(expected, collectRange(handle, collectorKey, from, leaf().maxDoc()));
    }

    // ── predicate provider: scorer path excludes deleted docs ──

    public void testPredicateExcludesDeletedDocsForDenseDeletions() throws Exception {
        buildIndex(1000, i -> i % 4 == 0); // 25% deleted → dense; docsIterator intersects with the live FixedBitSet
        assertDenseLiveDocs();
        LuceneFilterDelegationHandle handle = newHandle(List.of(termExpression(PREDICATE_ANNOTATION_ID, "tag", PREDICATE_TAG)));

        Set<Integer> expected = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0 && i % 4 != 0) { // matches tag=hello and is live
                expected.add(i);
            }
        }
        assertEquals(expected, collectAll(handle, PREDICATE_ANNOTATION_ID));
    }

    public void testPredicateExcludesDeletedDocsForSparseDeletions() throws Exception {
        Set<Integer> deleted = Set.of(2, 6, 10); // even (matching) docs, 0.3% → sparse; raw scorer + per-doc liveDocs.get
        buildIndex(1000, deleted::contains);
        assertSparseLiveDocs();
        LuceneFilterDelegationHandle handle = newHandle(List.of(termExpression(PREDICATE_ANNOTATION_ID, "tag", PREDICATE_TAG)));

        Set<Integer> expected = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0 && deleted.contains(i) == false) {
                expected.add(i);
            }
        }
        assertEquals(expected, collectAll(handle, PREDICATE_ANNOTATION_ID));
    }

    public void testPredicateWithoutDeletionsCollectsAllMatches() throws Exception {
        buildIndex(1000, i -> false); // no deletions → raw scorer, liveDocs == null (no per-doc filtering)
        assertNull("no deletions should leave liveDocs null", leaf().getLiveDocs());
        LuceneFilterDelegationHandle handle = newHandle(List.of(termExpression(PREDICATE_ANNOTATION_ID, "tag", PREDICATE_TAG)));

        Set<Integer> expected = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0) { // all tag=hello docs match and are live
                expected.add(i);
            }
        }
        assertEquals(expected, collectAll(handle, PREDICATE_ANNOTATION_ID));
    }

    public void testPredicateIterationIsStatefulAcrossRanges() throws Exception {
        buildIndex(1000, i -> i % 4 == 0); // dense
        LuceneFilterDelegationHandle handle = newHandle(List.of(termExpression(PREDICATE_ANNOTATION_ID, "tag", PREDICATE_TAG)));
        int providerKey = handle.createProvider(PREDICATE_ANNOTATION_ID);
        int collectorKey = handle.createCollector(providerKey, 1L, 0, leaf().maxDoc());

        // Two sequential sub-ranges over one collector must together cover the full live match set,
        // with no doc dropped or double-counted at the boundary (exercises the persisted currentDoc).
        int half = leaf().maxDoc() / 2;
        Set<Integer> collected = new TreeSet<>(collectRange(handle, collectorKey, 0, half));
        collected.addAll(collectRange(handle, collectorKey, half, leaf().maxDoc()));

        Set<Integer> expected = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0 && i % 4 != 0) {
                expected.add(i);
            }
        }
        assertEquals(expected, collected);
    }
}
