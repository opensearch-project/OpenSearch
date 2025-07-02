/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * An approximate-able version of term queries for numeric fields.
 * It creates an instance of {@link ApproximatePointRangeQuery} with the same lower and upper bounds.
 */
public class ApproximateTermQuery extends ApproximateQuery {

    private final ApproximatePointRangeQuery approximatePointRangeQuery;

    /**
     * Creates a new ApproximateTermQuery that wraps an ApproximatePointRangeQuery with the same lower and upper bounds.
     *
     * @param field The field name
     * @param pointValue The point value as bytes
     * @param numDims Number of dimensions
     * @param valueToString Function to convert bytes to string representation
     */
    public ApproximateTermQuery(String field, byte[] pointValue, int numDims, Function<byte[], String> valueToString) {
        this(field, pointValue, pointValue, numDims, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, null, valueToString);
    }

    /**
     * Creates a new ApproximateTermQuery that wraps an ApproximatePointRangeQuery with the same lower and upper bounds.
     *
     * @param field The field name
     * @param pointValue The point value as bytes
     * @param pointValue2 The second point value as bytes (same as pointValue for term queries)
     * @param numDims Number of dimensions
     * @param size Maximum number of documents to collect
     * @param sortOrder Sort order for document collection
     * @param valueToString Function to convert bytes to string representation
     */
    protected ApproximateTermQuery(
        String field,
        byte[] pointValue,
        byte[] pointValue2,
        int numDims,
        int size,
        SortOrder sortOrder,
        Function<byte[], String> valueToString
    ) {
        this.approximatePointRangeQuery = new ApproximatePointRangeQuery(
            field,
            pointValue,
            pointValue2,
            numDims,
            size,
            sortOrder,
            valueToString
        );
    }

    @Override
    public boolean canApproximate(SearchContext context) {
        return approximatePointRangeQuery.canApproximate(context);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        approximatePointRangeQuery.visit(visitor);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return approximatePointRangeQuery.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        return approximatePointRangeQuery.rewrite(indexSearcher);
    }

    public int getSize() {
        return approximatePointRangeQuery.getSize();
    }

    public void setSize(int size) {
        approximatePointRangeQuery.setSize(size);
    }

    public SortOrder getSortOrder() {
        return approximatePointRangeQuery.getSortOrder();
    }

    public void setSortOrder(SortOrder sortOrder) {
        approximatePointRangeQuery.setSortOrder(sortOrder);
    }

    @Override
    public int hashCode() {
        return approximatePointRangeQuery.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) && equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(ApproximateTermQuery other) {
        return Objects.equals(approximatePointRangeQuery, other.approximatePointRangeQuery);
    }

    @Override
    public String toString(String field) {
        final StringBuilder sb = new StringBuilder();
        sb.append("ApproximateTerm(");
        sb.append(approximatePointRangeQuery.toString());
        sb.append(")");
        return sb.toString();
    }
}
