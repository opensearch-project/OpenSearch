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
import org.apache.lucene.search.TermQuery;

import java.util.HashSet;
import java.util.Set;

public class WildcardFieldTypeTest extends FieldTypeTestCase {

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
        assertEquals(new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), "apple"), ft.termQuery("apple", null));
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

        assertEquals(
            new WildcardFieldMapper.WildcardMatchingQuery("field", builder.build(), pattern),
            ft.wildcardQuery(pattern, null, null)
        );
    }
}
