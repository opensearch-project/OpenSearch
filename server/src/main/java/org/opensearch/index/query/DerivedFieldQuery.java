/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexableField;
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
import org.opensearch.index.mapper.DerivedFieldValueFetcher;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * DerivedFieldQuery used for querying derived fields. It contains the logic to execute an input lucene query against
 * DerivedField. It also accepts DerivedFieldValueFetcher and SearchLookup as an input.
 */
public final class DerivedFieldQuery extends Query {
    private final Query query;
    private final DerivedFieldValueFetcher valueFetcher;
    private final SearchLookup searchLookup;
    private final Analyzer indexAnalyzer;
    private final boolean ignoreMalformed;

    private final Function<Object, IndexableField> indexableFieldGenerator;

    /**
     * @param query lucene query to be executed against the derived field
     * @param valueFetcher DerivedFieldValueFetcher ValueFetcher to fetch the value of a derived field from _source
     *                     using LeafSearchLookup
     * @param searchLookup SearchLookup to get the LeafSearchLookup look used by valueFetcher to fetch the _source
     */
    public DerivedFieldQuery(
        Query query,
        DerivedFieldValueFetcher valueFetcher,
        SearchLookup searchLookup,
        Analyzer indexAnalyzer,
        Function<Object, IndexableField> indexableFieldGenerator,
        boolean ignoreMalformed
    ) {
        this.query = query;
        this.valueFetcher = valueFetcher;
        this.searchLookup = searchLookup;
        this.indexAnalyzer = indexAnalyzer;
        this.indexableFieldGenerator = indexableFieldGenerator;
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        query.visit(visitor);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query rewritten = query.rewrite(indexSearcher);
        if (rewritten == query) {
            return this;
        }
        return new DerivedFieldQuery(rewritten, valueFetcher, searchLookup, indexAnalyzer, indexableFieldGenerator, ignoreMalformed);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) {
                DocIdSetIterator approximation;
                approximation = DocIdSetIterator.all(context.reader().maxDoc());
                valueFetcher.setNextReader(context);
                LeafSearchLookup leafSearchLookup = searchLookup.getLeafSearchLookup(context);
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() {
                        leafSearchLookup.source().setSegmentAndDocument(context, approximation.docID());
                        List<IndexableField> indexableFields;
                        try {
                            indexableFields = valueFetcher.getIndexableField(leafSearchLookup.source(), indexableFieldGenerator);
                        } catch (Exception e) {
                            if (ignoreMalformed) {
                                return false;
                            }
                            throw e;
                        }
                        MemoryIndex memoryIndex = new MemoryIndex();
                        for (IndexableField indexableField : indexableFields) {
                            memoryIndex.addField(indexableField, indexAnalyzer);
                        }
                        float score = memoryIndex.search(query);
                        return score > 0.0f;
                    }

                    @Override
                    public float matchCost() {
                        // TODO: how can we compute this?
                        return 1000f;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
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
        DerivedFieldQuery other = (DerivedFieldQuery) o;
        return Objects.equals(this.query, other.query) && Objects.equals(this.indexAnalyzer, other.indexAnalyzer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), query, indexAnalyzer);
    }

    @Override
    public String toString(String f) {
        return "DerivedFieldQuery (Query: [ " + query.toString(f) + "])";
    }
}
