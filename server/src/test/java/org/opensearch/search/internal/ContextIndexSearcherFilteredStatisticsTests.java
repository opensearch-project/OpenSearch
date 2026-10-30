/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.cache.filteredstats.FilteredStatsCache;
import org.opensearch.index.cache.filteredstats.IndicesFilteredStatsCache;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Statistics computed by {@link ContextIndexSearcher} when its {@link SearchContext} scopes them to a filter.
 * <p>
 * The oracle is a second index that physically contains only the documents the filter admits: every scoped statistic
 * that BM25 reads must equal what that index reports.
 */
public class ContextIndexSearcherFilteredStatisticsTests extends OpenSearchTestCase {

    private static final Query VISIBLE = new TermQuery(new Term("dept", "visible"));

    /** Visible documents, each written to both the scoped index and the reference index. */
    private static final List<String> VISIBLE_BODIES = List.of("alpha beta", "alpha", "gamma delta delta");

    /** Documents the filter excludes; "only_hidden" occurs in no visible document. */
    private static final List<String> HIDDEN_BODIES = List.of("alpha only_hidden", "alpha alpha", "beta only_hidden", "epsilon");

    private Directory scopedDir;
    private Directory referenceDir;
    private DirectoryReader scopedReader;
    private DirectoryReader referenceReader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        scopedDir = newDirectory();
        referenceDir = newDirectory();
        // Several segments, so the per-segment sums are exercised, and a deleted visible document, which the scoped
        // statistics must not count any more than the reference does.
        try (
            IndexWriter scoped = new IndexWriter(
                scopedDir,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            );
            IndexWriter reference = new IndexWriter(
                referenceDir,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < VISIBLE_BODIES.size(); i++) {
                scoped.addDocument(doc("visible", VISIBLE_BODIES.get(i), "v" + i));
                reference.addDocument(doc("visible", VISIBLE_BODIES.get(i), "v" + i));
                scoped.addDocument(doc("hidden", HIDDEN_BODIES.get(i), "h" + i));
                scoped.commit();
                reference.commit();
            }
            scoped.addDocument(doc("hidden", HIDDEN_BODIES.get(HIDDEN_BODIES.size() - 1), "h_last"));
            scoped.addDocument(doc("visible", "alpha removed", "gone"));
            scoped.deleteDocuments(new Term("id", "gone"));
            scoped.commit();
        }
        scopedReader = DirectoryReader.open(scopedDir);
        referenceReader = DirectoryReader.open(referenceDir);
        assertTrue("the scoped index must span several segments", scopedReader.leaves().size() > 1);
    }

    @Override
    public void tearDown() throws Exception {
        scopedReader.close();
        referenceReader.close();
        scopedDir.close();
        referenceDir.close();
        super.tearDown();
    }

    public void testScopedStatisticsMatchAPhysicallyFilteredIndex() throws IOException {
        assertMatchesReference(searcher(contextScopedTo(VISIBLE, null)));
    }

    public void testScopedStatisticsMatchThroughTheCache() throws IOException {
        FilteredStatsCache cache = new FilteredStatsCache(
            IndexSettingsModule.newIndexSettings("idx", Settings.EMPTY),
            new IndicesFilteredStatsCache(Settings.EMPTY, new NoopCircuitBreaker(CircuitBreaker.FIELDDATA))
        );
        // The second searcher reads what the first one stored, so both the fill and the hit are checked.
        assertMatchesReference(searcher(contextScopedTo(VISIBLE, cache)));
        assertMatchesReference(searcher(contextScopedTo(VISIBLE, cache)));
    }

    public void testATermOnlyInExcludedDocumentsGetsTheFloorLocallyAndNothingForDfs() throws IOException {
        ContextIndexSearcher searcher = searcher(contextScopedTo(VISIBLE, null));
        Term term = new Term("body", "only_hidden");

        TermStatistics local = searcher.termStatistics(term, 2, 2);
        assertEquals("smallest legal document frequency, never the excluded count", 1, local.docFreq());
        assertEquals(1, local.totalTermFreq());

        assertNull("the dfs phase must contribute nothing for the term", searcher.visibleSubsetTermStatistics(term, 2, 2));
    }

    public void testUnscopedContextReadsWholeIndexStatistics() throws IOException {
        SearchContext context = baseContext();
        when(context.useFilteredStatistics()).thenReturn(false);
        when(context.filteredStatisticsFilter()).thenReturn(VISIBLE);
        ContextIndexSearcher searcher = searcher(context);
        IndexSearcher whole = new IndexSearcher(scopedReader);

        Term alpha = new Term("body", "alpha");
        int docFreq = scopedReader.docFreq(alpha);
        long ttf = scopedReader.totalTermFreq(alpha);
        assertEquals(whole.termStatistics(alpha, docFreq, ttf).docFreq(), searcher.termStatistics(alpha, docFreq, ttf).docFreq());
        assertEquals(whole.collectionStatistics("body").docCount(), searcher.collectionStatistics("body").docCount());
        assertEquals(docFreq, searcher.visibleSubsetTermStatistics(alpha, docFreq, ttf).docFreq());
    }

    public void testScopedContextWithoutAFilterReadsWholeIndexStatistics() throws IOException {
        SearchContext context = baseContext();
        when(context.useFilteredStatistics()).thenReturn(true);
        when(context.filteredStatisticsFilter()).thenReturn(null);
        ContextIndexSearcher searcher = searcher(context);

        assertEquals(
            new IndexSearcher(scopedReader).collectionStatistics("body").docCount(),
            searcher.collectionStatistics("body").docCount()
        );
    }

    public void testQueryPhaseOfDfsReadsTheAggregatedStatistics() throws IOException {
        ContextIndexSearcher searcher = searcher(contextScopedTo(VISIBLE, null));
        Term alpha = new Term("body", "alpha");
        TermStatistics aggregatedTerm = new TermStatistics(alpha.bytes(), 7, 9);
        CollectionStatistics aggregatedField = new CollectionStatistics("body", 40, 30, 90, 60);
        searcher.setAggregatedDfs(new AggregatedDfs(Map.of(alpha, aggregatedTerm), Map.of("body", aggregatedField), 40));

        // The coordinator already aggregated scoped statistics, so the shard must not recompute them.
        assertSame(aggregatedTerm, searcher.termStatistics(alpha, 1, 1));
        assertSame(aggregatedField, searcher.collectionStatistics("body"));
        // The dfs entry point delegates to the shard's own statistics in this phase.
        int docFreq = scopedReader.docFreq(alpha);
        long ttf = scopedReader.totalTermFreq(alpha);
        assertEquals(docFreq, searcher.visibleSubsetTermStatistics(alpha, docFreq, ttf).docFreq());
    }

    public void testVisibleDocsAreChargedToTheRequestBreakerUntilTheContextCloses() throws IOException {
        final CircuitBreakerService breakers = breakerService(Settings.EMPTY);
        final List<Releasable> releasables = new ArrayList<>();
        final ContextIndexSearcher searcher = searcher(chargedContext(breakers, releasables));
        final CircuitBreaker request = breakers.getBreaker(CircuitBreaker.REQUEST);

        searcher.collectionStatistics("body");
        long expected = 0;
        for (LeafReaderContext leaf : scopedReader.leaves()) {
            expected += VisibleSubsetStatistics.visibleBitSetBytes(leaf.reader().maxDoc());
        }
        assertEquals("every segment's bitset is charged before it is built", expected, request.getUsed());

        // The bitsets are kept for the request, so later statistics do not charge again.
        searcher.termStatistics(new Term("body", "alpha"), 1, 1);
        assertEquals(expected, request.getUsed());
        assertEquals("one releasable returns the whole charge", 1, releasables.size());

        Releasables.close(releasables);
        assertEquals(0L, request.getUsed());
    }

    public void testABuildOverTheRequestBreakerFailsTheRequest() {
        final CircuitBreakerService breakers = breakerService(
            Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1b").build()
        );
        final List<Releasable> releasables = new ArrayList<>();
        final SearchContext context = chargedContext(breakers, releasables);
        expectThrows(CircuitBreakingException.class, () -> searcher(context).collectionStatistics("body"));
        Releasables.close(releasables);
        assertEquals("a refused build leaves nothing charged", 0L, breakers.getBreaker(CircuitBreaker.REQUEST).getUsed());
    }

    private static CircuitBreakerService breakerService(Settings settings) {
        return new HierarchyCircuitBreakerService(
            Settings.builder().put(settings).put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false).build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    /** A scoped context whose request breaker is the given service's, collecting the releasables it is handed. */
    private static SearchContext chargedContext(CircuitBreakerService breakers, List<Releasable> releasables) {
        final SearchContext context = contextScopedTo(VISIBLE, null);
        when(context.bigArrays()).thenReturn(new BigArrays(null, breakers, CircuitBreaker.REQUEST));
        doAnswer(invocation -> releasables.add(invocation.getArgument(0))).when(context).addReleasable(any());
        return context;
    }

    private void assertMatchesReference(ContextIndexSearcher searcher) throws IOException {
        IndexSearcher reference = new IndexSearcher(referenceReader);

        CollectionStatistics expectedField = reference.collectionStatistics("body");
        CollectionStatistics actualField = searcher.collectionStatistics("body");
        assertEquals("docCount", expectedField.docCount(), actualField.docCount());
        assertEquals("sumTotalTermFreq", expectedField.sumTotalTermFreq(), actualField.sumTotalTermFreq());

        for (String text : List.of("alpha", "beta", "delta", "gamma")) {
            Term term = new Term("body", text);
            TermStatistics expected = reference.termStatistics(term, referenceReader.docFreq(term), referenceReader.totalTermFreq(term));
            // The whole-index frequencies are passed in, as Lucene does; the scoped searcher must ignore them.
            int wholeDocFreq = Math.max(1, scopedReader.docFreq(term));
            long wholeTtf = Math.max(wholeDocFreq, scopedReader.totalTermFreq(term));
            TermStatistics actual = searcher.termStatistics(term, wholeDocFreq, wholeTtf);
            assertEquals(text + " docFreq", expected.docFreq(), actual.docFreq());
            assertEquals(text + " totalTermFreq", expected.totalTermFreq(), actual.totalTermFreq());
            assertEquals(
                text + " dfs docFreq",
                expected.docFreq(),
                searcher.visibleSubsetTermStatistics(term, wholeDocFreq, wholeTtf).docFreq()
            );
        }
    }

    private ContextIndexSearcher searcher(SearchContext context) throws IOException {
        return new ContextIndexSearcher(
            scopedReader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            context
        );
    }

    private static SearchContext contextScopedTo(Query filter, FilteredStatsCache cache) {
        SearchContext context = baseContext();
        when(context.useFilteredStatistics()).thenReturn(true);
        when(context.filteredStatisticsFilter()).thenReturn(filter);
        when(context.filteredStatsCache()).thenReturn(cache);
        return context;
    }

    private static SearchContext baseContext() {
        SearchContext context = mock(SearchContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(context.indexShard()).thenReturn(indexShard);
        when(indexShard.getSearchOperationListener()).thenReturn(new SearchOperationListener() {
        });
        when(context.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        return context;
    }

    private static Document doc(String dept, String body, String id) {
        Document doc = new Document();
        doc.add(new StringField("dept", dept, Field.Store.NO));
        doc.add(new StringField("id", id, Field.Store.NO));
        doc.add(new TextField("body", body, Field.Store.NO));
        return doc;
    }
}
