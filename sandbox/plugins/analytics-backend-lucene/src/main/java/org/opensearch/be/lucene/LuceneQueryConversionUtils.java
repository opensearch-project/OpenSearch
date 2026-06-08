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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermRangeQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for adapting Lucene {@link Query} trees so they can execute against the
 * <b>lucene-secondary</b> segment of a composite parquet-primary index.
 *
 * <p>The secondary segment stores only the inverted index (postings) for keyword / text /
 * match_only_text fields — their doc_values live in the parquet primary. Some queries that
 * OpenSearch builds assume doc_values are present on the same segment they execute against;
 * those need rewriting to a postings-only equivalent here.
 */
public final class LuceneQueryConversionUtils {

    private LuceneQueryConversionUtils() {}

    /**
     * Rewrite every {@link FieldExistsQuery} in {@code query} to a postings-only existence query.
     *
     * <p><b>Why:</b> {@code MappedFieldType.existsQuery} emits a {@link FieldExistsQuery} whenever
     * the mapping declares doc_values. On the secondary segment those doc_values are absent, and
     * {@code FieldExistsQuery.rewrite()} throws ("indexes neither doc values, norms nor vectors").
     * A {@link TermRangeQuery} with null bounds matches any doc that has ≥1 indexed term for the
     * field — semantically equivalent to "field exists" for the postings-indexed keyword/text/
     * match_only_text types that filter delegation supports.
     *
     * <p>This reaches us via the {@code query_string} the PPL {@code search} command compiles for
     * {@code !=} / {@code NOT} / {@code _exists_} (e.g. {@code "_exists_:f AND NOT f:v"}). The
     * {@code QueryStringQueryParser} can nest the {@link FieldExistsQuery} under Boolean (AND/OR/NOT),
     * ConstantScore, Boost, or DisjunctionMax (multi-field) wrappers, so we recurse through all of
     * them and rebuild only the branches that changed (reference-equality short-circuit). Any other
     * container that still hides a {@link FieldExistsQuery} fails fast with an actionable message
     * rather than the cryptic Lucene error at {@code searcher.rewrite()}.
     *
     * <p><b>Interim:</b> the clean long-term fix is for the secondary to read doc_values out of the
     * parquet primary, after which {@link FieldExistsQuery} resolves natively and this rewrite can
     * be deleted.
     *
     * @param query the compiled Lucene query (non-null)
     * @return an equivalent query with all {@link FieldExistsQuery}s rewritten; the same instance
     *         when nothing changed
     */
    public static Query rewriteFieldExistsForSecondary(Query query) {
        if (query instanceof FieldExistsQuery fieldExists) {
            // null lower/upper bound = unbounded both ends = "any term present for this field".
            return new TermRangeQuery(fieldExists.getField(), null, null, true, true);
        }
        if (query instanceof ConstantScoreQuery constantScore) {
            Query inner = rewriteFieldExistsForSecondary(constantScore.getQuery());
            return inner == constantScore.getQuery() ? constantScore : new ConstantScoreQuery(inner);
        }
        if (query instanceof BoostQuery boost) {
            Query inner = rewriteFieldExistsForSecondary(boost.getQuery());
            return inner == boost.getQuery() ? boost : new BoostQuery(inner, boost.getBoost());
        }
        if (query instanceof BooleanQuery bool) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setMinimumNumberShouldMatch(bool.getMinimumNumberShouldMatch());
            boolean changed = false;
            for (BooleanClause clause : bool.clauses()) {
                Query rewritten = rewriteFieldExistsForSecondary(clause.query());
                changed |= rewritten != clause.query();
                builder.add(rewritten, clause.occur());
            }
            return changed ? builder.build() : bool;
        }
        if (query instanceof DisjunctionMaxQuery disjunctionMax) {
            List<Query> rewritten = new ArrayList<>(disjunctionMax.getDisjuncts().size());
            boolean changed = false;
            for (Query disjunct : disjunctionMax.getDisjuncts()) {
                Query r = rewriteFieldExistsForSecondary(disjunct);
                changed |= r != disjunct;
                rewritten.add(r);
            }
            return changed ? new DisjunctionMaxQuery(rewritten, disjunctionMax.getTieBreakerMultiplier()) : disjunctionMax;
        }
        if (containsFieldExists(query)) {
            throw new IllegalStateException(
                "Unhandled query container wrapping a FieldExistsQuery on a doc-values-less secondary "
                    + "segment; rewriteFieldExistsForSecondary must cover "
                    + query.getClass().getName()
                    + ": "
                    + query
            );
        }
        return query;
    }

    /** True if {@code query} contains a {@link FieldExistsQuery} anywhere in its subtree. */
    static boolean containsFieldExists(Query query) {
        boolean[] found = { false };
        query.visit(new QueryVisitor() {
            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this; // descend into every clause regardless of occur
            }

            @Override
            public void visitLeaf(Query leaf) {
                if (leaf instanceof FieldExistsQuery) {
                    found[0] = true;
                }
            }

            @Override
            public void consumeTerms(Query q, Term... terms) {
                if (q instanceof FieldExistsQuery) {
                    found[0] = true;
                }
            }
        });
        return found[0];
    }
}
