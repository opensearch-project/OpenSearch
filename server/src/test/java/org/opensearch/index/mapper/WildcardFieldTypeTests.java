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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.HashSet;
import java.util.Set;

public class WildcardFieldTypeTests extends FieldTypeTestCase {

    static String prefixAnchored(String val) {
        return (char) 0 + val;
    }

    static String suffixAnchored(String val) {
        return val + (char) 0;
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

        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "\\*"),
            ft.wildcardQuery("\\*", null, null)
        );

        expectedTerms.remove(suffixAnchored("*"));
        builder = new BooleanQuery.Builder();
        for (String term : expectedTerms) {
            builder.add(new TermQuery(new Term("field", term)), BooleanClause.Occur.FILTER);
        }
        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "\\**"),
            ft.wildcardQuery("\\**", null, null)
        );
    }

    public void testMultipleWildcardsInQuery() {
        final String pattern = "a?cd*efg?h";
        MappedFieldType ft = new WildcardFieldMapper.WildcardFieldType("field");
        Set<String> expectedTerms = new HashSet<>();
        expectedTerms.add(prefixAnchored("a"));
        expectedTerms.add("cd");
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

        pattern = "ab(zz|cd|ef.*)(hi|jk)";
        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "ab")), BooleanClause.Occur.FILTER);
        builder.add(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "zz")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("field", "cd")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("field", "ef")), BooleanClause.Occur.SHOULD)
                .build(),
            BooleanClause.Occur.FILTER
        );
        builder.add(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "hi")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("field", "jk")), BooleanClause.Occur.SHOULD)
                .build(),
            BooleanClause.Occur.FILTER
        );
        actual = ft.regexpQuery(pattern, 0, 0, 1000, null, null);
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "/" + pattern + "/"), actual);
        actualMatchingQuery = (WildcardFieldMapper.WildcardMatchingQuery) actual;
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abcdjk"));
        assertTrue(actualMatchingQuery.getSecondPhaseMatcher().test("abefqwertyhi"));
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
