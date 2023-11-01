/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A query that matches against each document from the parent query by filtering using the source field values.
 * Useful to query against field type which doesn't store positional data and field is not stored/computed dynamically.
 */
public class SourceFieldMatchQuery extends Query {
    final private Query delegateQuery;
    final private Query filter;
    final private SearchLookup lookup;
    final private MappedFieldType fieldType;
    final private SourceValueFetcher valueFetcher;

    /**
     * Constructs a SourceFieldMatchQuery.
     *
     * @param delegateQuery The parent query to use to find matches.
     * @param filter The query used to filter further by running against field value computed using _source field.
     * @param fieldType The mapped field type.
     * @param valueFetcher The source value fetcher.
     * @param lookup The search lookup.
     */
    public SourceFieldMatchQuery(
        Query delegateQuery,
        Query filter,
        MappedFieldType fieldType,
        SourceValueFetcher valueFetcher,
        SearchLookup lookup
    ) {
        this.delegateQuery = delegateQuery;
        this.filter = filter;
        this.fieldType = fieldType;
        this.valueFetcher = valueFetcher;
        this.lookup = lookup;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        delegateQuery.visit(visitor);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        return delegateQuery.rewrite(searcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

        Weight weight = delegateQuery.createWeight(searcher, scoreMode, boost);

        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {

                Scorer scorer = weight.scorer(context);
                DocIdSetIterator approximation = scorer.iterator();
                LeafSearchLookup leafSearchLookup = lookup.getLeafSearchLookup(context);
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {

                    @Override
                    public boolean matches() {
                        leafSearchLookup.setDocument(approximation.docID());
                        List<Object> values = valueFetcher.fetchValues(leafSearchLookup.source());
                        MemoryIndex memoryIndex = new MemoryIndex();
                        for (Object value : values) {
                            memoryIndex.addField(fieldType.name(), (String) value, fieldType.indexAnalyzer());
                        }
                        float score = memoryIndex.search(delegateQuery);
                        return score > 0.0f;
                    }

                    @Override
                    public float matchCost() {
                        // arbitrary cost
                        return 1000f;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // It is fine to cache if delegate query weight is cacheable since additional logic here
                // is just a filter on top of delegate query matches
                return weight.isCacheable(ctx);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        SourceFieldMatchQuery other = (SourceFieldMatchQuery) o;
        return Objects.equals(this.delegateQuery, other.delegateQuery)
            && this.filter == other.filter
            && Objects.equals(this.lookup, other.lookup)
            && Objects.equals(this.fieldType, other.fieldType)
            && Objects.equals(this.valueFetcher, other.valueFetcher);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), delegateQuery, filter, lookup, fieldType, valueFetcher);
    }

    @Override
    public String toString(String f) {
        return "SourceFieldMatchQuery (delegate query: [ " + delegateQuery.toString(f) + " ], filter query: [ " + filter.toString(f) + "])";
    }
}
