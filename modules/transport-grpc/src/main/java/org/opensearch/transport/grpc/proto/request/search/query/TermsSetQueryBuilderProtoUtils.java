/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.TermsSetQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting TermsSet query Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of terms_set queries
 * into their corresponding OpenSearch TermsSetQueryBuilder implementations for search operations.
 */
class TermsSetQueryBuilderProtoUtils {

    private TermsSetQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer TermsSetQuery to an OpenSearch TermsSetQueryBuilder.
     * Similar to {@link TermsSetQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * TermsSetQueryBuilder with the appropriate field name, terms, boost, query name,
     * and minimum should match settings.
     *
     * @param termsSetQueryProto The Protocol Buffer TermsSetQuery object
     * @return A configured TermsSetQueryBuilder instance
     * @throws IllegalArgumentException if the terms_set query is invalid or missing required fields
     */
    static TermsSetQueryBuilder fromProto(org.opensearch.protobufs.TermsSetQuery termsSetQueryProto) {
        if (termsSetQueryProto == null) {
            throw new IllegalArgumentException("TermsSetQuery must not be null");
        }

        if (termsSetQueryProto.getField() == null || termsSetQueryProto.getField().isEmpty()) {
            throw new IllegalArgumentException("Field name is required for TermsSetQuery");
        }

        if (termsSetQueryProto.getTermsCount() == 0) {
            throw new IllegalArgumentException("At least one term is required for TermsSetQuery");
        }

        String fieldName = termsSetQueryProto.getField();
        List<String> values = new ArrayList<>(termsSetQueryProto.getTermsList());
        String minimumShouldMatchField = null;
        Script minimumShouldMatchScript = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        if (termsSetQueryProto.hasBoost()) {
            boost = termsSetQueryProto.getBoost();
        }

        if (termsSetQueryProto.hasXName()) {
            queryName = termsSetQueryProto.getXName();
        }

        if (termsSetQueryProto.hasMinimumShouldMatchField()) {
            minimumShouldMatchField = termsSetQueryProto.getMinimumShouldMatchField();
        }

        if (termsSetQueryProto.hasMinimumShouldMatchScript()) {
            minimumShouldMatchScript = ScriptProtoUtils.parseFromProtoRequest(termsSetQueryProto.getMinimumShouldMatchScript());
        }

        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(fieldName, values);

        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);

        if (minimumShouldMatchField != null) {
            queryBuilder.setMinimumShouldMatchField(minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            queryBuilder.setMinimumShouldMatchScript(minimumShouldMatchScript);
        }

        return queryBuilder;
    }
}
