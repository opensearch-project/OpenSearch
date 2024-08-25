/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Query class for querying star tree data structure.
 *
 * @opensearch.experimental
 */
public class StarTreeQuery extends Query implements Accountable {

    /**
     * Star tree field info
     * This is used to get the star tree data structure
     */
    CompositeIndexFieldInfo starTree;

    /**
     * Map of field name to a list of predicates to be applied on that field
     * This is used to filter the data based on the predicates
     */
    Map<String, List<Predicate<Long>>> compositePredicateMap;

    public StarTreeQuery(CompositeIndexFieldInfo starTree, Map<String, List<Predicate<Long>>> compositePredicateMap) {
        this.starTree = starTree;
        this.compositePredicateMap = compositePredicateMap;
    }

    @Override
    public String toString(String field) {
        return null;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                SegmentReader reader = Lucene.segmentReader(context.reader());

                // We get the 'CompositeIndexReader' instance so that we can get StarTreeValues
                if (!(reader.getDocValuesReader() instanceof CompositeIndexReader)) return null;

                CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
                List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
                StarTreeValues starTreeValues = null;
                if (compositeIndexFields != null && !compositeIndexFields.isEmpty()) {
                    starTreeValues = (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(starTree);
                } else {
                    return null;
                }

                StarTreeFilter filter = new StarTreeFilter(starTreeValues, compositePredicateMap);
                DocIdSetIterator result = filter.getStarTreeResult();
                return new ConstantScoreScorer(this, score(), scoreMode, result);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    public CompositeIndexFieldInfo getStarTree() {
        return starTree;
    }
}
