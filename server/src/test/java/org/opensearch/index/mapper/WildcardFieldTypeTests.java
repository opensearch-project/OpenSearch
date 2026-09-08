/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.HashSet;
import java.util.Set;

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

    // Test wildcard queries uses prefix queries Without three ngram Terms query
    public void testWildcardQueryUsesPrefixQuery() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new PrefixQuery(new Term("field", "a")), BooleanClause.Occur.FILTER);
        builder.add(new PrefixQuery(new Term("field", "b")), BooleanClause.Occur.FILTER);

        String pattern = "*a*b*";
        Query actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), pattern), actual);
        WildcardFieldMapper.WildcardMatchingQuery actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("zzazbzz"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("zzbza"));

        pattern = "*ab*cde*";
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "cde")), BooleanClause.Occur.FILTER);

        actual = ft.wildcardQuery(pattern, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), pattern), actual);
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

    // Test regexp queries uses prefix queries Without three ngram Terms query
    public void testRegexpQueryUsesPrefixQuery() {
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        String pattern = ".*ab.*a.*";

        Query actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", new PrefixQuery(new Term("field", "ab")), "/" + pattern + "/"),
            actual
        );
        WildcardFieldMapper.WildcardMatchingQuery actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("foo_ab_a"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("foo_a_ab"));

        pattern = ".*(ab|cd).*";
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", ft.existsQuery(null), "/" + pattern + "/"), actual);
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("xxabxx"));
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("xxcdxx"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("xxefxx"));

        pattern = ".*a.*cde.*";
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", new TermQuery(new Term("field", "cde")), "/" + pattern + "/"),
            actual
        );
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("foo_a_cde"));
        assertFalse(actualMatchingQuery.getSecondPhaseMatcher().test("foo_cde_a"));
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
