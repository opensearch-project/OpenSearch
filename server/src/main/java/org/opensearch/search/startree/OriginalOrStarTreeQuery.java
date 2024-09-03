/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * Preserves star-tree queries which can be used along with original query
 * Decides which star-tree query to use (or not) based on cost factors
 *
 * @opensearch.experimental
 */
public class OriginalOrStarTreeQuery extends Query {

    private final StarTreeQuery starTreeQuery;
    private final Query originalQuery;
    private boolean starTreeQueryUsed;

    public OriginalOrStarTreeQuery(StarTreeQuery starTreeQuery, Query originalQuery) {
        this.starTreeQuery = starTreeQuery;
        this.originalQuery = originalQuery;
        this.starTreeQueryUsed = false;
    }

    @Override
    public String toString(String s) {
        return originalQuery.toString(s);
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {}

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) && equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(OriginalOrStarTreeQuery other) {
        return starTreeQuery.equals(other.starTreeQuery) && originalQuery.equals(other.originalQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), starTreeQuery, originalQuery, starTreeQuery);
    }

    public boolean isStarTreeUsed() {
        return starTreeQueryUsed;
    }

    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (searcher.getIndexReader().hasDeletions() == false) {
            this.starTreeQueryUsed = true;
            return this.starTreeQuery.createWeight(searcher, scoreMode, boost);
        } else {
            return this.originalQuery.createWeight(searcher, scoreMode, boost);
        }
    }

    public StarTreeQuery getStarTreeQuery() {
        return starTreeQuery;
    }
}
