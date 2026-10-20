/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link LuceneQueryConversionUtils#rewriteFieldExistsForSecondary} — the
 * interim rewrite that turns {@link FieldExistsQuery} (needs doc_values/norms, absent on the
 * lucene-secondary segment) into a postings-only {@link TermRangeQuery}.
 */
public class LuceneQueryConversionUtilsTests extends OpenSearchTestCase {

    private static FieldExistsQuery exists(String field) {
        return new FieldExistsQuery(field);
    }

    /** A TermRangeQuery over {@code field} with both bounds null = "any term present". */
    private static void assertExistsRewrite(Query rewritten, String field) {
        assertThat(rewritten, instanceOf(TermRangeQuery.class));
        TermRangeQuery trq = (TermRangeQuery) rewritten;
        assertEquals(field, trq.getField());
        assertNull("lower bound should be unbounded", trq.getLowerTerm());
        assertNull("upper bound should be unbounded", trq.getUpperTerm());
    }

    public void testBareFieldExistsIsRewritten() {
        assertExistsRewrite(LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(exists("severityText")), "severityText");
    }

    public void testFieldExistsUnderConstantScore() {
        Query in = new ConstantScoreQuery(exists("f"));
        Query out = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(in);
        assertThat(out, instanceOf(ConstantScoreQuery.class));
        assertExistsRewrite(((ConstantScoreQuery) out).getQuery(), "f");
    }

    public void testFieldExistsUnderBoost() {
        Query in = new BoostQuery(exists("f"), 2.0f);
        Query out = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(in);
        assertThat(out, instanceOf(BoostQuery.class));
        assertEquals(2.0f, ((BoostQuery) out).getBoost(), 0.0f);
        assertExistsRewrite(((BoostQuery) out).getQuery(), "f");
    }

    /** The q19/search shape: "_exists_:f AND NOT f:v" → Boolean{MUST exists, MUST_NOT term}. */
    public void testFieldExistsUnderBooleanAndNot() {
        BooleanQuery in = new BooleanQuery.Builder().add(exists("severityText"), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("severityText", "INFO")), BooleanClause.Occur.MUST_NOT)
            .build();
        Query out = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(in);
        assertThat(out, instanceOf(BooleanQuery.class));
        BooleanQuery bool = (BooleanQuery) out;
        assertEquals(2, bool.clauses().size());
        for (BooleanClause clause : bool.clauses()) {
            if (clause.occur() == BooleanClause.Occur.MUST) {
                assertExistsRewrite(clause.query(), "severityText");
            } else {
                assertThat(clause.query(), instanceOf(TermQuery.class)); // untouched
            }
        }
    }

    public void testFieldExistsUnderDisjunctionMax() {
        DisjunctionMaxQuery in = new DisjunctionMaxQuery(List.of(exists("a"), new TermQuery(new Term("b", "x"))), 0.3f);
        Query out = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(in);
        assertThat(out, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery dm = (DisjunctionMaxQuery) out;
        assertEquals(0.3f, dm.getTieBreakerMultiplier(), 0.0f);
        boolean sawRange = dm.getDisjuncts().stream().anyMatch(q -> q instanceof TermRangeQuery);
        assertTrue("a FieldExistsQuery disjunct should have become a TermRangeQuery", sawRange);
    }

    public void testNestedWrappers() {
        Query in = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(new BoostQuery(exists("deep"), 1.5f), BooleanClause.Occur.SHOULD).build()
        );
        Query out = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(in);
        // unwrap CS -> Boolean -> Boost -> TermRangeQuery
        BooleanQuery bool = (BooleanQuery) ((ConstantScoreQuery) out).getQuery();
        BoostQuery boost = (BoostQuery) bool.clauses().get(0).query();
        assertExistsRewrite(boost.getQuery(), "deep");
    }

    /** No FieldExistsQuery anywhere → same instance returned (no needless rebuild). */
    public void testUnaffectedQueryReturnedAsSameInstance() {
        Query in = new BooleanQuery.Builder().add(new TermQuery(new Term("f", "v")), BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
            .build();
        assertSame(in, LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(in));
    }

    public void testLeafNonExistsReturnedAsSameInstance() {
        Query in = new TermQuery(new Term("f", "v"));
        assertSame(in, LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(in));
    }

    public void testContainsFieldExistsDetection() {
        assertTrue(LuceneQueryConversionUtils.containsFieldExists(new ConstantScoreQuery(exists("f"))));
        assertFalse(LuceneQueryConversionUtils.containsFieldExists(new TermQuery(new Term("f", "v"))));
    }
}
