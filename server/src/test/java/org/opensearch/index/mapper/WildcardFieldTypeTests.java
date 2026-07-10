/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WildcardFieldTypeTests extends FieldTypeTestCase {

    static String prefixAnchored(String val) {
        String ret = (char) 0 + val;
        if (ret.length() < WildcardFieldMapper.NGRAM_SIZE) {
            ret = prefixAnchored(ret);
        }
        return ret;
    }

    static String suffixAnchored(String val) {
        String ret = val + (char) 0;
        if (ret.length() < WildcardFieldMapper.NGRAM_SIZE) {
            ret = suffixAnchored(ret);
        }
        return ret;
    }

    public void testTermQuery() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Set<String> expectedTerms = new HashSet<>();
        expectedTerms.add(prefixAnchored("ap"));
        expectedTerms.add("app");
        expectedTerms.add("ppl");
        expectedTerms.add("ple");
        expectedTerms.add(suffixAnchored("le"));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        Query actual = ft.termQuery("apple", null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "apple"), actual);
        WildcardFieldMapper.WildcardMatchingQuery actualTermQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualTermQuery.getSecondPhaseMatcher().test("apple"));
        assertFalse(actualTermQuery.getSecondPhaseMatcher().test("Apple"));
        assertFalse(actualTermQuery.getSecondPhaseMatcher().test("flapple"));
        assertFalse(actualTermQuery.getSecondPhaseMatcher().test("apples"));
    }

    public void testWildcardQuery() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Set<String> expectedTerms = new HashSet<>();
        expectedTerms.add(prefixAnchored("ap"));
        expectedTerms.add("app");
        expectedTerms.add("ppl");
        expectedTerms.add("ple");
        expectedTerms.add(suffixAnchored("le"));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }

        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "apple"),
            ft.wildcardQuery("apple", null, null)
        );

        expectedTerms.remove(prefixAnchored("ap"));
        builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "*apple"),
            ft.wildcardQuery("*apple", null, null)
        );

        expectedTerms.remove(suffixAnchored("le"));
        builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "*apple*"),
            ft.wildcardQuery("*apple*", null, null)
        );
    }

    public void testEscapedWildcardQuery() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Set<String> expectedTerms = new HashSet<>();
        expectedTerms.add(prefixAnchored("*"));
        expectedTerms.add(suffixAnchored("*"));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }

        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "\\**\\*"),
            ft.wildcardQuery("\\**\\*", null, null)
        );

        expectedTerms.add(prefixAnchored("*" + (char) 0));
        builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "\\*"), ft.wildcardQuery("\\*", null, null));
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", prefixAnchored("*"))), BooleanClause.Occur.FILTER);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "\\**"), ft.wildcardQuery("\\**", null, null));
    }

    public void testMultipleWildcardsInQuery() {
        final String pattern = "a?cd*efg?h";
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Set<String> expectedTerms = new HashSet<>();
        expectedTerms.add(prefixAnchored("a"));
        expectedTerms.add("efg");
        expectedTerms.add(suffixAnchored("h"));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }

        Query actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), pattern), actual);
        WildcardFieldMapper.WildcardMatchingQuery actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abcdzzzefgqh"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("abcdzzzefgqqh"));
    }

    public void testEscapedBackslashFollowedByWildcard() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");

        // Test case from issue #19719
        // Pattern: *some\\* means "wildcard + 'some\' + wildcard"
        // Should match strings like "some\string", "awesome\stuff", etc.

        // Verify ngram generation doesn't include wildcard characters
        Set<String> ngrams = WildcardFieldMapper.WildcardFieldType.getRequiredNGrams("*some\\\\*", false);
        assertFalse("Ngrams should not contain wildcard characters", ngrams.stream().anyMatch(s -> s.contains("*")));
        assertTrue(ngrams.contains("som"));
        assertTrue(ngrams.contains("ome"));
        assertTrue(ngrams.contains("me\\"));

        // Test the query
        Query query = ft.wildcardQuery("*some\\\\*", null, null);
        assertTrue(query instanceof WildcardFieldMapper.WildcardMatchingQuery);

        WildcardFieldMapper.WildcardMatchingQuery wildcardQuery = (WildcardFieldMapper.WildcardMatchingQuery) query;

        // Second phase matcher should correctly match strings with backslash
        assertTrue(wildcardQuery.getSecondPhaseMatcher().test("some\\string"));
        assertTrue(wildcardQuery.getSecondPhaseMatcher().test("some\\"));
        assertTrue(wildcardQuery.getSecondPhaseMatcher().test("prefix_some\\suffix"));

        // Should not match strings without backslash
        assertFalse(wildcardQuery.getSecondPhaseMatcher().test("somestring"));
        assertFalse(wildcardQuery.getSecondPhaseMatcher().test("some/string"));
    }

    public void testRegexpQuery() {
        String pattern = ".*apple.*";
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");

        Set<String> expectedTerms = new HashSet<>();
        expectedTerms.add("app");
        expectedTerms.add("ppl");
        expectedTerms.add("ple");
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }

        Query actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "/" + pattern + "/"), actual);
        WildcardFieldMapper.WildcardMatchingQuery actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("foo_apple_foo"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("foo_apply_foo"));

        pattern = "abc(zzz|def|ghi.*)(jkl|mno)";
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "abc")), BooleanClause.Occur.FILTER);
        builder.add(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "zzz")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("field", "def")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("field", "ghi")), BooleanClause.Occur.SHOULD)
                .build(),
            BooleanClause.Occur.FILTER
        );
        builder.add(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "jkl")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("field", "mno")), BooleanClause.Occur.SHOULD)
                .build(),
            BooleanClause.Occur.FILTER
        );
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "/" + pattern + "/"), actual);
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abcdefmno"));
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abcghiqwertyjkl"));
    }

    public void testWildcardMatchAll() {
        String pattern = "???";
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Query actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", ft.existsQuery(null), "???"), actual);

        pattern = "*";
        actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(ft.existsQuery(null), actual);
    }

    public void testCacheabilityDependsOnSearchLookup() throws IOException {
        BooleanQuery firstPhase = firstPhaseQueryForApple();

        // A query built without a QueryShardContext holds no shard-scoped state and stays cacheable.
        WildcardFieldMapper.WildcardMatchingQuery contextFreeQuery = new WildcardFieldMapper.WildcardMatchingQuery(
            "field",
            firstPhase,
            "*apple*"
        );
        WildcardFieldMapper.WildcardMatchingQuery contextBoundQuery = new WildcardFieldMapper.WildcardMatchingQuery(
            "field",
            firstPhase,
            s -> s.contains("apple"),
            "*apple*",
            mockedQueryShardContext(),
            new WildcardFieldMapper.WildcardFieldType("field")
        );

        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.KEYWORD_ANALYZER));
            iw.addDocument(appleTrigramDocument());
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);

                Weight contextFreeWeight = contextFreeQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                assertTrue(contextFreeWeight.isCacheable(reader.leaves().get(0)));

                // A query bound to a QueryShardContext captures the shard's SearchLookup. Caching its weight
                // would pin that lookup (and everything reachable from it) in the query cache.
                Weight contextBoundWeight = contextBoundQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                assertFalse(contextBoundWeight.isCacheable(reader.leaves().get(0)));
            }
        }
    }

    public void testFirstPhaseQueryCachedIndependently() throws IOException {
        QueryShardContext context = mockedQueryShardContext();
        BooleanQuery firstPhase = firstPhaseQueryForApple();

        WildcardFieldMapper.WildcardMatchingQuery query = new WildcardFieldMapper.WildcardMatchingQuery(
            "field",
            firstPhase,
            s -> s.contains("apple"),
            "*apple*",
            context,
            new WildcardFieldMapper.WildcardFieldType("field")
        );

        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.KEYWORD_ANALYZER));
            iw.addDocument(appleTrigramDocument());
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);
                List<Query> cachedQueries = new ArrayList<>();
                LRUQueryCache cache = new LRUQueryCache(1000, 1 << 20, leaf -> true, 1f) {
                    @Override
                    protected void onQueryCache(Query query, long ramBytesUsed) {
                        super.onQueryCache(query, ramBytesUsed);
                        cachedQueries.add(query);
                    }
                };
                searcher.setQueryCache(cache);
                searcher.setQueryCachingPolicy(new QueryCachingPolicy() {
                    @Override
                    public void onUse(Query query) {}

                    @Override
                    public boolean shouldCache(Query query) {
                        return true;
                    }
                });

                assertEquals(1, searcher.count(query));
                // The first-phase trigram query (and its clauses) is cached; the outer two-phase query,
                // which holds the shard-scoped SearchLookup, must not enter the cache.
                assertTrue(cachedQueries.contains(firstPhase));
                for (Query cached : cachedQueries) {
                    assertFalse(cached instanceof WildcardFieldMapper.WildcardMatchingQuery);
                }

                assertEquals(1, searcher.count(query));
                assertTrue(cache.getHitCount() > 0);
            }
        }
    }

    /**
     * Regression test for <a href="https://github.com/opensearch-project/OpenSearch/issues/22419">#22419</a>.
     * Prior to the fix, the two-phase {@code WildcardMatchingQuery} declared itself cacheable while holding a
     * reference to the shard-scoped {@link SearchLookup}. Once inserted into the query cache (whose entries
     * outlive the search), the cache pinned the lookup and everything reachable from it on the heap, and since
     * the query's equals/hashCode ignore the lookup, repeated wildcard searches accumulated distinct pinned
     * object graphs until the node ran out of heap.
     */
    public void testSearchLookupNotPinnedByQueryCache() throws Exception {
        QueryShardContext context = mockedQueryShardContext();
        WeakReference<SearchLookup> lookupRef = new WeakReference<>(context.lookup());

        Query query = new WildcardFieldMapper.WildcardMatchingQuery(
            "field",
            firstPhaseQueryForApple(),
            s -> s.contains("apple"),
            "*apple*",
            context,
            new WildcardFieldMapper.WildcardFieldType("field")
        );
        context = null;

        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.KEYWORD_ANALYZER));
            iw.addDocument(appleTrigramDocument());
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                IndexSearcher searcher = new IndexSearcher(reader);
                LRUQueryCache cache = new LRUQueryCache(1000, 1 << 20, leaf -> true, 1f);
                searcher.setQueryCache(cache);
                searcher.setQueryCachingPolicy(new QueryCachingPolicy() {
                    @Override
                    public void onUse(Query query) {}

                    @Override
                    public boolean shouldCache(Query query) {
                        return true;
                    }
                });

                assertEquals(1, searcher.count(query));
                assertTrue("expected the first-phase query to be cached", cache.getCacheCount() > 0);

                // Drop every reference to the query except whatever the cache retained. If the cache pinned
                // the query (and, through it, the SearchLookup), the weak reference never clears and this
                // assertion times out, reproducing the heap pinning reported in #22419.
                query = null;
                assertBusy(() -> {
                    System.gc(); // GC is not deterministic, hence the polling
                    assertNull("SearchLookup is still strongly reachable: the query cache is pinning shard state", lookupRef.get());
                }, 5, TimeUnit.SECONDS);
            }
        }
    }

    private static BooleanQuery firstPhaseQueryForApple() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : Set.of("app", "ppl", "ple")) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    private static Document appleTrigramDocument() {
        Document doc = new Document();
        for (String term : Set.of("app", "ppl", "ple")) {
            doc.add(new StringField("field", term, Field.Store.NO));
        }
        return doc;
    }

    private static QueryShardContext mockedQueryShardContext() {
        QueryShardContext context = mock(QueryShardContext.class);
        SearchLookup searchLookup = mock(SearchLookup.class);
        LeafSearchLookup leafSearchLookup = mock(LeafSearchLookup.class);
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(Map.of("field", "there is an apple here"));
        when(leafSearchLookup.source()).thenReturn(sourceLookup);
        when(searchLookup.getLeafSearchLookup(any())).thenReturn(leafSearchLookup);
        when(context.lookup()).thenReturn(searchLookup);
        when(context.sourcePath("field")).thenReturn(Set.of("field"));
        return context;
    }

    public void testRegexpMatchAll() {
        // The following matches any string of length exactly 3. We do need to evaluate the predicate.
        String pattern = "...";
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Query actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", ft.existsQuery(null), "/.../"), actual);

        // The following pattern has a predicate that matches everything. We can just return the field exists query.
        pattern = ".*";
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(ft.existsQuery(null), actual);
    }
}
