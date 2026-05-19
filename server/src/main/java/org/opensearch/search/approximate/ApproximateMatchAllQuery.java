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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Replaces match-all query with a less expensive query if possible.
 * <p>
 * Currently, will rewrite to a bounded range query over the high/low end of a field if a primary sort is specified
 * on that field.
 */
public class ApproximateMatchAllQuery extends ApproximateQuery {
    private ApproximateQuery approximation = null;

    @Override
    protected boolean canApproximate(SearchContext context) {
        approximation = null;
        if (context == null) {
            return false;
        }
        if (context.aggregations() != null) {
            return false;
        }
        // Exclude approximation when "track_total_hits": true
        if (context.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return false;
        }

        if (context.request() != null && context.request().source() != null && context.innerHits().getInnerHits().isEmpty()) {
            if (context.request().source().sorts() != null && context.request().source().sorts().size() > 1) {
                return false;
            }
            FieldSortBuilder primarySortField = FieldSortBuilder.getPrimaryFieldSortOrNull(context.request().source());
            if (primarySortField != null
                && primarySortField.missing() == null
                && !primarySortField.fieldName().equals(FieldSortBuilder.DOC_FIELD_NAME)
                && !primarySortField.fieldName().equals(FieldSortBuilder.ID_FIELD_NAME)) {
                MappedFieldType mappedFieldType = context.getQueryShardContext().fieldMapper(primarySortField.fieldName());
                if (mappedFieldType == null) {
                    return false;
                }
                Query rangeQuery = mappedFieldType.rangeQuery(null, null, false, false, null, null, null, context.getQueryShardContext());
                if (rangeQuery instanceof ApproximateScoreQuery approximateScoreQuery) {
                    approximateScoreQuery.setContext(context);
                    if (approximateScoreQuery.resolvedQuery instanceof ApproximateQuery) {
                        approximation = (ApproximateQuery) approximateScoreQuery.resolvedQuery;
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public String toString(String field) {
        return "Approximate(*:*)";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);

    }

    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o)) {
            ApproximateMatchAllQuery other = (ApproximateMatchAllQuery) o;
            return Objects.equals(approximation, other.approximation);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (approximation == null) {
            throw new IllegalStateException("rewrite called without setting context or query could not be approximated");
        }
        return approximation.rewrite(indexSearcher);
    }
}
