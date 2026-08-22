/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * A Lucene {@link Query} that matches documents in which a given field does <em>not</em> exist.
 * <p>
 * This is the complement of a field-exists query. It is intended to be used as a positive
 * filter clause (e.g. in a {@code FILTER} position inside a {@link org.apache.lucene.search.BooleanQuery})
 * rather than as a {@code MUST_NOT} clause, so that it is eligible for Lucene's
 * {@link org.apache.lucene.search.LRUQueryCache}.
 * <p>
 * Execution model: for each segment, an inner {@link Weight} is created for the wrapped
 * exists query. The scorer returns a {@link ComplementDocIdSetIterator} that walks all
 * document IDs in the segment and skips those matched by the inner iterator.
 *
 * @opensearch.internal
 */
public class NotExistsQuery extends Query {

    private final Query innerExistsQuery;
    private final String fieldName;

    /**
     * @param innerExistsQuery the positive exists query whose complement this query represents
     * @param fieldName        the field name, used only for {@link #toString} and {@link #equals}
     */
    public NotExistsQuery(Query innerExistsQuery, String fieldName) {
        this.innerExistsQuery = Objects.requireNonNull(innerExistsQuery, "innerExistsQuery must not be null");
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
    }

    public Query getInnerExistsQuery() {
        return innerExistsQuery;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight innerWeight = innerExistsQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);

        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                int maxDoc = context.reader().maxDoc();
                Scorer innerScorer = innerWeight.scorer(context);

                if (innerScorer == null) {
                    // The inner exists query matches no documents in this segment, meaning
                    // the field is absent from all docs → every doc matches "not exists".
                    return new ConstantScoreScorer(this, score(), scoreMode, DocIdSetIterator.all(maxDoc));
                }

                DocIdSetIterator complementIter = new ComplementDocIdSetIterator(innerScorer.iterator(), maxDoc);
                return new ConstantScoreScorer(this, score(), scoreMode, complementIter);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return innerWeight.isCacheable(ctx);
            }
        };
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query rewrittenInner = innerExistsQuery.rewrite(indexSearcher);
        if (rewrittenInner != innerExistsQuery) {
            return new NotExistsQuery(rewrittenInner, fieldName);
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            innerExistsQuery.visit(visitor.getSubVisitor(null, this));
        }
    }

    @Override
    public String toString(String field) {
        return "NotExists(" + fieldName + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof NotExistsQuery other)) return false;
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(innerExistsQuery, other.innerExistsQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, innerExistsQuery);
    }

    /**
     * A {@link DocIdSetIterator} that returns all document IDs in [0, maxDoc) that are
     * <em>not</em> matched by the wrapped inner iterator.
     */
    static final class ComplementDocIdSetIterator extends DocIdSetIterator {

        private final DocIdSetIterator inner;
        private final int maxDoc;
        private int doc = -1;
        /** The next doc ID in the inner (exists) iterator; NO_MORE_DOCS when exhausted. */
        private int nextInner = -1;

        ComplementDocIdSetIterator(DocIdSetIterator inner, int maxDoc) {
            this.inner = inner;
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            doc = target;
            while (doc < maxDoc) {
                // Make sure nextInner is >= doc
                if (nextInner < doc) {
                    nextInner = inner.advance(doc);
                }
                if (nextInner == doc) {
                    // This doc IS in the inner iterator (field exists) — skip it.
                    doc++;
                } else {
                    // nextInner > doc: this doc is NOT in the inner iterator — it matches.
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            // Approximate: total docs minus the cost of the inner (exists) iterator.
            return Math.max(0, maxDoc - inner.cost());
        }
    }
}
