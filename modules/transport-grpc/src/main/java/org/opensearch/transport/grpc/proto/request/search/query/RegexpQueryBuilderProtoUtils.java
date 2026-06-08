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
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.protobufs.RegexpQuery;

/**
 * Utility class for converting RegexpQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of regexp queries
 * into their corresponding OpenSearch RegexpQueryBuilder implementations for search operations.
 */
class RegexpQueryBuilderProtoUtils {

    private RegexpQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer RegexpQuery to an OpenSearch RegexpQueryBuilder.
     * Similar to {@link RegexpQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * RegexpQueryBuilder with the appropriate field name, value, boost, query name,
     * flags, case sensitivity, max determinized states, and rewrite method.
     *
     * @param regexpQueryProto The Protocol Buffer RegexpQuery object
     * @return A configured RegexpQueryBuilder instance
     * @throws IllegalArgumentException if the regexp query is null or missing required fields
     */
    static RegexpQueryBuilder fromProto(RegexpQuery regexpQueryProto) {
        String fieldName = regexpQueryProto.getField();
        String rewrite = null;
        String value = regexpQueryProto.getValue();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int flagsValue = RegexpQueryBuilder.DEFAULT_FLAGS_VALUE;
        boolean caseInsensitive = RegexpQueryBuilder.DEFAULT_CASE_INSENSITIVITY;
        int maxDeterminizedStates = RegexpQueryBuilder.DEFAULT_DETERMINIZE_WORK_LIMIT;
        String queryName = null;

        if (regexpQueryProto.hasBoost()) {
            boost = regexpQueryProto.getBoost();
        }

        if (regexpQueryProto.hasRewrite()) {
            rewrite = regexpQueryProto.getRewrite();
        }

        if (regexpQueryProto.hasFlags()) {
            // Convert string flags to integer value using RegexpFlag.resolveValue
            flagsValue = org.opensearch.index.query.RegexpFlag.resolveValue(regexpQueryProto.getFlags());
        }

        if (regexpQueryProto.hasMaxDeterminizedStates()) {
            maxDeterminizedStates = regexpQueryProto.getMaxDeterminizedStates();
        }

        if (regexpQueryProto.hasCaseInsensitive()) {
            caseInsensitive = regexpQueryProto.getCaseInsensitive();
        }

        if (regexpQueryProto.hasXName()) {
            queryName = regexpQueryProto.getXName();
        }

        RegexpQueryBuilder result = new RegexpQueryBuilder(fieldName, value).flags(flagsValue)
            .maxDeterminizedStates(maxDeterminizedStates)
            .rewrite(rewrite)
            .boost(boost)
            .queryName(queryName);
        result.caseInsensitive(caseInsensitive);
        return result;
    }
}
