/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * A wrapper around {@link IndexOrDocValuesQuery} that can be used to run approximate queries.
 * It delegates to either {@link ApproximateableQuery} or {@link IndexOrDocValuesQuery} based on whether the query can be approximated or not.
 * @see ApproximateableQuery
 */
public final class ApproximateIndexOrDocValuesQuery extends ApproximateScoreQuery {

    private final ApproximateableQuery approximateIndexQuery;
    private final IndexOrDocValuesQuery indexOrDocValuesQuery;

    public ApproximateIndexOrDocValuesQuery(Query indexQuery, ApproximateableQuery approximateIndexQuery, Query dvQuery) {
        super(new IndexOrDocValuesQuery(indexQuery, dvQuery), approximateIndexQuery);
        this.approximateIndexQuery = approximateIndexQuery;
        this.indexOrDocValuesQuery = new IndexOrDocValuesQuery(indexQuery, dvQuery);
    }

    @Override
    public String toString(String field) {
        return "ApproximateIndexOrDocValuesQuery(indexQuery="
            + indexOrDocValuesQuery.getIndexQuery().toString(field)
            + ", approximateIndexQuery="
            + approximateIndexQuery.toString(field)
            + ", dvQuery="
            + indexOrDocValuesQuery.getRandomAccessQuery().toString(field)
            + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        indexOrDocValuesQuery.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + indexOrDocValuesQuery.getIndexQuery().hashCode();
        h = 31 * h + indexOrDocValuesQuery.getRandomAccessQuery().hashCode();
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (approximateIndexQuery.canApproximate(this.getContext())) {
            return approximateIndexQuery.createWeight(searcher, scoreMode, boost);
        }
        return indexOrDocValuesQuery.createWeight(searcher, scoreMode, boost);
    }
}
