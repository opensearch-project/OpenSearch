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
import org.apache.lucene.util.Accountable;

import java.io.IOException;

/**
 * Preserves star-tree queries which can be used along with original query
 * Decides which star-tree query to use (or not) based on cost factors
 *
 * @opensearch.experimental
 */
public class OriginalOrStarTreeQuery extends Query implements Accountable {

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
        return "";
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {

    }

    @Override
    public boolean equals(Object o) {
        return true;
    }

    @Override
    public int hashCode() {
        return originalQuery.hashCode();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
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

    public Query getOriginalQuery() {
        return originalQuery;
    }

    public StarTreeQuery getStarTreeQuery() {
        return starTreeQuery;
    }
}
