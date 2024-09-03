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
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Query class for querying star tree data structure.
 *
 * @opensearch.experimental
 */
public class StarTreeQuery extends Query {

    /**
     * Star tree field info
     * This is used to get the star tree data structure
     */
    CompositeIndexFieldInfo starTree;

    /**
     * Map of field name to a value to be queried for that field
     * This is used to filter the data based on the query
     */
    Map<String, Long> queryMap;

    public StarTreeQuery(CompositeIndexFieldInfo starTree, Map<String, Long> queryMap) {
        this.starTree = starTree;
        this.queryMap = queryMap;
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj) && equalsTo(getClass().cast(obj));
    }

    private boolean equalsTo(StarTreeQuery other) {
        return starTree.equals(other.starTree) && queryMap != null && queryMap.equals(other.queryMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), starTree, queryMap);
    }

    @Override
    public String toString(String field) {
        // Does not implement a user-readable toString
        return null;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                StarTreeValues starTreeValues = getStarTreeValues(context);
                if (starTreeValues == null) {
                    return null;
                }
                StarTreeFilter filter = new StarTreeFilter(starTreeValues, queryMap);
                DocIdSetIterator result = filter.getStarTreeResult();
                return new ConstantScoreScorer(this, score(), scoreMode, result);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                try {
                    return getStarTreeValues(ctx).getStarTreeDocumentCount() < ctx.reader().maxDoc();
                } catch (Exception suppressed) {
                    assert false : "Not able to check cacheable criteria";
                    return false;
                }
            }

            private StarTreeValues getStarTreeValues(LeafReaderContext ctx) throws IOException {
                SegmentReader reader = Lucene.segmentReader(ctx.reader());
                CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
                List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
                if (compositeIndexFields != null && !compositeIndexFields.isEmpty()) {
                    return (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(starTree);
                } else {
                    return null;
                }
            }
        };
    }

    public CompositeIndexFieldInfo getStarTree() {
        return starTree;
    }
}
