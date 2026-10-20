/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
        assertEquals(expectedQuery("field", builder.build(), "apple"), actual);
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

        assertEquals(expectedQuery("field", builder.build(), "apple"), ft.wildcardQuery("apple", null, null));

        expectedTerms.remove(prefixAnchored("ap"));
        builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        assertEquals(expectedQuery("field", builder.build(), "*apple"), ft.wildcardQuery("*apple", null, null));

        expectedTerms.remove(suffixAnchored("le"));
        builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        assertEquals(expectedQuery("field", builder.build(), "*apple*"), ft.wildcardQuery("*apple*", null, null));
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

        assertEquals(expectedQuery("field", builder.build(), "\\**\\*"), ft.wildcardQuery("\\**\\*", null, null));

        expectedTerms.add(prefixAnchored("*" + (char) 0));
        builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        assertEquals(expectedQuery("field", builder.build(), "\\*"), ft.wildcardQuery("\\*", null, null));
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", prefixAnchored("*"))), BooleanClause.Occur.FILTER);
        assertEquals(expectedQuery("field", builder.build(), "\\**"), ft.wildcardQuery("\\**", null, null));
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
        assertEquals(expectedQuery("field", builder.build(), pattern), actual);
        WildcardFieldMapper.WildcardMatchingQuery actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abcdzzzefgqh"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("abcdzzzefgqqh"));
    }

    // Test wildcard queries uses prefix queries Without three ngram Terms query
    public void testWildcardQueryUsesPrefixQuery() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new PrefixQuery(new Term("field", "a")), BooleanClause.Occur.FILTER);
        builder.add(new PrefixQuery(new Term("field", "b")), BooleanClause.Occur.FILTER);

        String pattern = "*a*b*";
        Query actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(expectedQuery("field", builder.build(), pattern), actual);
        WildcardFieldMapper.WildcardMatchingQuery actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("zzazbzz"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("zzbza"));

        pattern = "*ab*cde*";
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "cde")), BooleanClause.Occur.FILTER);

        actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(expectedQuery("field", builder.build(), pattern), actual);
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("zzabzzcdezz"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("zzcdezzabzz"));
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
        assertEquals(expectedQuery("field", builder.build(), "/" + pattern + "/"), actual);
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
        assertEquals(expectedQuery("field", builder.build(), "/" + pattern + "/"), actual);
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abcdefmno"));
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abcghiqwertyjkl"));
    }

    // Test regexp queries uses prefix queries Without three ngram Terms query
    public void testRegexpQueryUsesPrefixQuery() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        String pattern = ".*ab.*a.*";

        Query actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(expectedQuery("field", new PrefixQuery(new Term("field", "ab")), "/" + pattern + "/"), actual);
        WildcardFieldMapper.WildcardMatchingQuery actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("foo_ab_a"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("foo_a_ab"));

        pattern = ".*(ab|cd).*";
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(expectedQuery("field", ft.existsQuery(null), "/" + pattern + "/"), actual);
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("xxabxx"));
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("xxcdxx"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("xxefxx"));

        pattern = ".*a.*cde.*";
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(expectedQuery("field", new TermQuery(new Term("field", "cde")), "/" + pattern + "/"), actual);
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("foo_a_cde"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("foo_cde_a"));
    }

    public void testWildcardMatchAll() {
        String pattern = "???";
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Query actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(expectedQuery("field", ft.existsQuery(null), "???"), actual);

        pattern = "*";
        actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(ft.existsQuery(null), actual);
    }

    public void testWildcardMatchingQueryEquality() {
        BooleanQuery bq = new BooleanQuery.Builder().build();
        WildcardFieldMapper.WildcardMatchingQuery q1 = expectedQuery("field", bq, "test*");
        WildcardFieldMapper.WildcardMatchingQuery q2 = expectedQuery("field", bq, "test*");
        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());

        WildcardFieldMapper.WildcardMatchingQuery q3 = expectedQuery("field", bq, "other*");
        assertNotEquals(q1, q3);

        WildcardFieldMapper.WildcardMatchingQuery q4 = expectedQuery("other_field", bq, "test*");
        assertNotEquals(q1, q4);
    }

    /**
     * The query is admitted to the query cache, so two queries that compare equal must select the same documents.
     * The second-phase matcher is a lambda that cannot be compared, so the flags it was compiled with have to be
     * part of the identity.
     */
    public void testFlagsAffectEquality() {
        Query firstPhase = new TermQuery(new Term("field", "abc"));
        Predicate<String> anyMatcher = s -> true;
        WildcardFieldMapper.WildcardMatchingQuery base = new WildcardFieldMapper.WildcardMatchingQuery(
            "field",
            firstPhase,
            anyMatcher,
            "abc",
            0,
            0,
            null,
            null
        );
        assertEquals(base, new WildcardFieldMapper.WildcardMatchingQuery("field", firstPhase, anyMatcher, "abc", 0, 0, null, null));
        assertEquals(
            base.hashCode(),
            new WildcardFieldMapper.WildcardMatchingQuery("field", firstPhase, anyMatcher, "abc", 0, 0, null, null).hashCode()
        );
        assertNotEquals(
            base,
            new WildcardFieldMapper.WildcardMatchingQuery(
                "field",
                firstPhase,
                anyMatcher,
                "abc",
                0,
                RegExp.ASCII_CASE_INSENSITIVE,
                null,
                null
            )
        );
        assertNotEquals(
            base,
            new WildcardFieldMapper.WildcardMatchingQuery("field", firstPhase, anyMatcher, "abc", RegExp.INTERVAL, 0, null, null)
        );
    }

    /**
     * A regexp union of single characters approximates with the field's exists query regardless of case sensitivity
     * and keeps the same pattern string, which leaves the match flags as the only thing telling the two apart.
     */
    public void testCaseInsensitiveRegexpQueryIsNotEqualToCaseSensitiveOne() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        String pattern = "a|b";
        Query caseSensitive = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        Query caseInsensitive = ft.regexpQuery(pattern, 0, RegExp.ASCII_CASE_INSENSITIVE, 1000, null, null);

        // Same field, same approximation, same pattern string.
        Query sameFieldAndApproximation = expectedQuery("field", ft.existsQuery(null), "/" + pattern + "/");
        assertEquals(sameFieldAndApproximation, caseSensitive);
        assertNotEquals(sameFieldAndApproximation, caseInsensitive);
        assertNotEquals(caseSensitive, caseInsensitive);

        // ... but they verify candidates differently.
        assertFalse(((WildcardFieldMapper.WildcardMatchingQuery) caseSensitive).getSecondPhaseMatcher().test("A"));
        assertTrue(((WildcardFieldMapper.WildcardMatchingQuery) caseInsensitive).getSecondPhaseMatcher().test("A"));
    }

    /**
     * Wildcard patterns with a literal already get a case-dependent first phase, so the two queries differ today
     * even without the flags. The flags keep them apart should the approximation ever become case-agnostic.
     */
    public void testCaseInsensitiveWildcardQueryIsNotEqualToCaseSensitiveOne() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Query caseSensitive = ft.wildcardQuery("*A*", null, false, null);
        Query caseInsensitive = ft.wildcardQuery("*A*", null, true, null);

        assertNotEquals(caseSensitive, caseInsensitive);
        assertFalse(((WildcardFieldMapper.WildcardMatchingQuery) caseSensitive).getSecondPhaseMatcher().test("xax"));
        assertTrue(((WildcardFieldMapper.WildcardMatchingQuery) caseInsensitive).getSecondPhaseMatcher().test("xax"));
    }

    /**
     * The value fetcher used for second-phase verification must be fully resolved when the query is built. A
     * {@link WildcardFieldMapper.WildcardMatchingQuery} outlives its request once the query cache keeps it as a
     * cache key, so reaching back into the {@link QueryShardContext} at scorer time would pin the shard's
     * {@code IndexSearcher} and segment readers on the heap. See issue 22419.
     */
    public void testValueFetcherSupplierResolvesSourcePathsUpFront() throws IOException {
        WildcardFieldMapper.WildcardFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.sourcePath("field")).thenReturn(Set.of("field"));

        Supplier<ValueFetcher> supplier = ft.valueFetcherSupplier(context);
        verify(context).sourcePath("field");
        clearInvocations(context);

        SourceLookup lookup = new SourceLookup();
        lookup.setSource(Map.of("field", "apple"));
        assertEquals(List.of("apple"), supplier.get().fetchValues(lookup));
        assertEquals(List.of("apple"), supplier.get().fetchValues(lookup));
        assertEquals(ft.valueFetcher(context, null, null).fetchValues(lookup), supplier.get().fetchValues(lookup));
        clearInvocations(context);
        assertEquals(List.of("apple"), supplier.get().fetchValues(lookup));
        verifyNoMoreInteractions(context);
    }

    /**
     * Doc-values enabled fields fetch through {@link DocValueFetcher}, exactly as
     * {@link WildcardFieldMapper.WildcardFieldType#valueFetcher} already did for the query-phase lookup. The
     * {@link IndexFieldData} is index-level state, resolved once when the query is built.
     */
    public void testValueFetcherSupplierUsesDocValuesWhenAvailable() {
        WildcardFieldMapper.Builder builder = new WildcardFieldMapper.Builder("field").docValues(true);
        WildcardFieldMapper.WildcardFieldType ft = new WildcardFieldMapper.WildcardFieldType("field", Lucene.KEYWORD_ANALYZER, builder);
        assertTrue(ft.hasDocValues());

        QueryShardContext context = mock(QueryShardContext.class);
        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        doReturn(fieldData).when(context).getForField(ft);

        Supplier<ValueFetcher> supplier = ft.valueFetcherSupplier(context);
        verify(context).getForField(ft);
        clearInvocations(context);

        assertThat(supplier.get(), instanceOf(DocValueFetcher.class));
        assertThat(supplier.get(), instanceOf(DocValueFetcher.class));
        verifyNoMoreInteractions(context);
    }

    /**
     * End-to-end check that a query which entered the query cache still verifies candidates against field values on
     * a later request, and that it never calls back into the {@link QueryShardContext} that built it.
     */
    public void testSecondPhaseSurvivesQueryCaching() throws IOException {
        WildcardFieldMapper.WildcardFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.sourcePath("field")).thenReturn(Set.of("field"));

        // The first phase can only require the "ple" trigram, so "purple" is a candidate that only the second
        // phase can reject.
        String pattern = "*a?ple*";
        assertEquals(Set.of("ple"), WildcardFieldMapper.WildcardFieldType.getRequiredNGrams(pattern, false));

        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
                writer.addDocument(wildcardFieldDocument("apple"));
                writer.addDocument(wildcardFieldDocument("purple"));
                writer.addDocument(wildcardFieldDocument("banana"));
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LRUQueryCache cache = new LRUQueryCache(100, 100_000, leaf -> true, 1f);
                searcher.setQueryCache(cache);
                searcher.setQueryCachingPolicy(new QueryCachingPolicy() {
                    @Override
                    public void onUse(Query query) {}

                    @Override
                    public boolean shouldCache(Query query) {
                        return true;
                    }
                });

                Query query = ft.wildcardQuery(pattern, null, false, context);
                Query equalQuery = ft.wildcardQuery(pattern, null, false, context);
                assertEquals(query, equalQuery);

                WildcardFieldMapper.WildcardMatchingQuery matchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) query;
                assertTrue(matchingQuery.getSecondPhaseMatcher().test("apple"));
                assertFalse(matchingQuery.getSecondPhaseMatcher().test("purple"));

                clearInvocations(context);

                assertEquals(1, countMatches(searcher, query));
                long entriesAfterFirstRun = cache.getCacheCount();
                assertThat(entriesAfterFirstRun, greaterThan(0L));

                // An equal query built by a later request must be answered from the cache with the same documents.
                assertEquals(1, countMatches(searcher, equalQuery));
                assertEquals(entriesAfterFirstRun, cache.getCacheCount());
                assertThat(cache.getHitCount(), greaterThan(0L));

                // Neither run consulted the query shard context.
                verifyNoMoreInteractions(context);
            }
        }
    }

    /**
     * A query built without a {@link QueryShardContext} has no way to fetch field values, so it can be compared but
     * never searched. Searching one has to fail loudly rather than silently returning first-phase false positives.
     */
    public void testQueryWithoutShardContextCannotBeSearched() throws IOException {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
                writer.addDocument(wildcardFieldDocument("apple"));
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Query noContext = ft.wildcardQuery("*a?ple*", null, null);
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> countMatches(searcher, noContext));
                assertThat(e.getMessage(), containsString("built without a QueryShardContext"));
            }
        }
    }

    private static int countMatches(IndexSearcher searcher, Query query) throws IOException {
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        int count = 0;
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier == null) {
                continue;
            }
            DocIdSetIterator iterator = scorerSupplier.get(Long.MAX_VALUE).iterator();
            while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                count++;
            }
        }
        return count;
    }

    /** Indexes a value the way {@link WildcardFieldMapper} does: trigrams for the first phase plus {@code _source}. */
    private static Document wildcardFieldDocument(String value) {
        FieldType ngramFieldType = new FieldType();
        ngramFieldType.setIndexOptions(IndexOptions.DOCS);
        ngramFieldType.setTokenized(true);
        ngramFieldType.setOmitNorms(true);
        ngramFieldType.freeze();

        Tokenizer tokenizer = new WildcardFieldMapper.WildcardFieldTokenizer();
        tokenizer.setReader(new StringReader(value));

        Document document = new Document();
        document.add(new Field("field", tokenizer, ngramFieldType));
        document.add(new StoredField(SourceFieldMapper.NAME, new BytesRef("{\"field\":\"" + value + "\"}")));
        return document;
    }

    public void testRegexpMatchAll() {
        // The following matches any string of length exactly 3. We do need to evaluate the predicate.
        String pattern = "...";
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Query actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(expectedQuery("field", ft.existsQuery(null), "/.../"), actual);

        // The following pattern has a predicate that matches everything. We can just return the field exists query.
        pattern = ".*";
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(ft.existsQuery(null), actual);
    }

    /**
     * Builds the query the field type is expected to produce, for equality assertions only. The matcher is a
     * placeholder because it takes no part in {@code equals}, and there is no shard context, so the result cannot
     * be searched.
     */
    private static WildcardFieldMapper.WildcardMatchingQuery expectedQuery(String field, Query firstPhase, String pattern) {
        return new WildcardFieldMapper.WildcardMatchingQuery(field, firstPhase, s -> true, pattern, 0, 0, null, null);
    }
}
