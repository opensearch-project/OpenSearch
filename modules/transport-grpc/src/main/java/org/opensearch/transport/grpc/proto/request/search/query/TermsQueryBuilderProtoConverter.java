/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.protobufs.QueryContainer;

import java.util.Map;

/**
 * Converter for Terms queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Terms query support
 * for the gRPC transport module.
 */
public class TermsQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new TermsQueryBuilderProtoConverter.
     */
    public TermsQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.TERMS;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasTerms()) {
            throw new IllegalArgumentException("QueryContainer does not contain a Terms query");
        }

        // Extract the first (and should be only) entry from the terms map
        org.opensearch.protobufs.TermsQuery termsQuery = queryContainer.getTerms();
        if (termsQuery.getTermsCount() != 1) {
            throw new IllegalArgumentException("TermsQuery must contain exactly one field, found: " + termsQuery.getTermsCount());
        }

        // Get the first entry from the map
        Map.Entry<String, org.opensearch.protobufs.TermsQueryField> entry = termsQuery.getTermsMap().entrySet().iterator().next();
        String fieldName = entry.getKey();
        org.opensearch.protobufs.TermsQueryField termsQueryField = entry.getValue();

        org.opensearch.protobufs.TermsQueryValueType vt = termsQuery.hasValueType()
            ? termsQuery.getValueType()
            : org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT;

        TermsQueryBuilder builder = TermsQueryBuilderProtoUtils.fromProto(fieldName, termsQueryField, vt);

        if (termsQuery.hasBoost()) {
            builder.boost(termsQuery.getBoost());
        }
        if (termsQuery.hasUnderscoreName()) {
            builder.queryName(termsQuery.getUnderscoreName());
        }

        return builder;
    }
}
