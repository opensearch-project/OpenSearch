/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.protobufs.RegexpQuery;

import java.util.Locale;

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
        String field = regexpQueryProto.getField();
        String value = regexpQueryProto.getValue();

        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder(field, value);

        // Set optional parameters
        if (regexpQueryProto.hasBoost()) {
            regexpQueryBuilder.boost(regexpQueryProto.getBoost());
        }

        if (regexpQueryProto.hasXName()) {
            regexpQueryBuilder.queryName(regexpQueryProto.getXName());
        }

        if (regexpQueryProto.hasFlags()) {
            // Convert string flags to integer value using RegexpFlag.resolveValue
            regexpQueryBuilder.flags(org.opensearch.index.query.RegexpFlag.resolveValue(regexpQueryProto.getFlags()));
        }

        if (regexpQueryProto.hasCaseInsensitive()) {
            regexpQueryBuilder.caseInsensitive(regexpQueryProto.getCaseInsensitive());
        }

        if (regexpQueryProto.hasMaxDeterminizedStates()) {
            regexpQueryBuilder.maxDeterminizedStates(regexpQueryProto.getMaxDeterminizedStates());
        }

        if (regexpQueryProto.hasRewrite()) {
            RegexpQuery.MultiTermQueryRewrite rewriteEnum = regexpQueryProto.getRewrite();

            // Skip setting rewrite method if it's UNSPECIFIED
            if (rewriteEnum != RegexpQuery.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_UNSPECIFIED) {
                String rewriteMethod = rewriteEnum.name();
                // Remove the prefix and convert to lowercase to match expected format
                rewriteMethod = rewriteMethod.replace("MULTI_TERM_QUERY_REWRITE_", "").toLowerCase(Locale.ROOT);
                regexpQueryBuilder.rewrite(rewriteMethod);
            }
        }

        return regexpQueryBuilder;
    }
}
