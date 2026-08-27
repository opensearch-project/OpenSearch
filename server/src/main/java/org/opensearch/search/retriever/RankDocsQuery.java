/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Internal-only query that replays a pre-computed retriever ranking onto an ordinary search.
 * <p>
 * The coordinator resolves the retriever tree to a list of {@link RankDoc}s — each a
 * {@code (index, shardId, _id, score, position)}. The whole window is broadcast to every shard as the
 * {@link RankDocsQueryBuilder}; {@link RankDocsQueryBuilder#doToQuery} then scopes it to this shard's
 * {@code (index, shardId)} <em>before</em> building this query, so the query holds only its own shard's
 * docs. It seeks each {@code _id} in the {@code _id} terms dictionary and hands back the document's
 * <em>pre-computed</em> score. Ordering by {@code position} (when order is decoupled from score) is
 * handled separately by {@link RankDocsSortField}; this query only carries {@code _id}s and scores.
 * <p>
 * This query has no scoring logic of its own — it replays a ranking the coordinator already computed —
 * so it does <b>not</b> support {@code explain}: {@code Weight.explain} throws
 * {@link UnsupportedOperationException}. The user-facing {@code _explanation} is the coordinator-built
 * retriever tree, and the retriever framework never runs the final {@code RankDocsQuery} search with
 * explain enabled.
 *
 * @opensearch.internal
 */
public final class RankDocsQuery extends Query {

    private final List<RankDoc> rankDocs;
    private final String indexName;
    private final int shardId;

    /**
     * @param rankDocs  this shard's slice of the resolved window ({@code (index, shardId)}-scoped by
     *                  {@link RankDocsQueryBuilder#doToQuery}); MUST already be immutable
     * @param indexName the concrete index this shard belongs to
     * @param shardId   this shard's id within {@code indexName}
     */
    public RankDocsQuery(List<RankDoc> rankDocs, String indexName, int shardId) {
        // The list is already an immutable, shard-scoped copy from the builder; store the reference directly.
        this.rankDocs = Objects.requireNonNull(rankDocs, "rankDocs");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
        this.shardId = shardId;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                // RankDocsQuery only replays a ranking the retriever already computed on the coordinator;
                // it has no scoring logic of its own to explain, and the retriever framework never issues
                // the final RankDocsQuery search with explain enabled (the user-facing _explanation is the
                // coordinator-built retriever tree, which overwrites any per-query explanation). There is
                // no valid case in which this is called, so fail loudly rather than emit a misleading one.
                throw new UnsupportedOperationException(
                    "RankDocsQuery does not support explain; the retriever framework builds the explanation "
                        + "on the coordinator and never runs the final RankDocsQuery search with explain enabled"
                );
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final RankDocsResolver.Resolved resolved = RankDocsResolver.resolve(context, rankDocs);
                if (resolved.size() == 0) {
                    return null;
                }
                final float[] scores = new float[resolved.size()];
                for (int i = 0; i < resolved.size(); i++) {
                    scores[i] = resolved.docs[i].score() * boost;
                }
                final Scorer scorer = new RankDocsScorer(resolved.docIds, scores);
                return new DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Matches/scores depend on a per-request window shipped from the coordinator, not on stable
                // segment content, so this query must never be cached in the query cache.
                return false;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "RankDocsQuery(index=" + indexName + ", shardId=" + shardId + ", docs=" + rankDocs.size() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o) == false) {
            return false;
        }
        RankDocsQuery other = (RankDocsQuery) o;
        return shardId == other.shardId && indexName.equals(other.indexName) && rankDocs.equals(other.rankDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), rankDocs, indexName, shardId);
    }

    /**
     * A {@link Scorer} that walks a sorted array of Lucene doc ids and returns each one's pre-computed score.
     * {@code docIds} MUST be sorted ascending; {@code scores[i]} is the score for {@code docIds[i]}.
     */
    static final class RankDocsScorer extends Scorer {
        private final int[] docIds;
        private final float[] scores;
        private final ArrayDocIdSetIterator iterator;

        RankDocsScorer(int[] docIds, float[] scores) {
            this.docIds = docIds;
            this.scores = scores;
            this.iterator = new ArrayDocIdSetIterator(docIds);
        }

        @Override
        public int docID() {
            return iterator.docID();
        }

        @Override
        public float score() {
            final int idx = iterator.currentIndex();
            if (idx < 0 || idx >= scores.length) {
                return 0f;
            }
            return scores[idx];
        }

        @Override
        public float getMaxScore(int upTo) {
            float max = 0f;
            for (int i = 0; i < docIds.length && docIds[i] <= upTo; i++) {
                max = Math.max(max, scores[i]);
            }
            return max;
        }

        @Override
        public DocIdSetIterator iterator() {
            return iterator;
        }
    }

    /** A forward-only {@link DocIdSetIterator} over a sorted, de-duplicated array of doc ids. */
    static final class ArrayDocIdSetIterator extends DocIdSetIterator {
        private final int[] docIds;
        private int index = -1;
        private int doc = -1;

        ArrayDocIdSetIterator(int[] docIds) {
            this.docIds = docIds;
        }

        int currentIndex() {
            return index;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            int lo = Math.max(index, 0);
            while (lo < docIds.length && docIds[lo] < target) {
                lo++;
            }
            if (lo >= docIds.length) {
                index = docIds.length;
                doc = NO_MORE_DOCS;
            } else {
                index = lo;
                doc = docIds[lo];
            }
            return doc;
        }

        @Override
        public long cost() {
            return docIds.length;
        }
    }
}
