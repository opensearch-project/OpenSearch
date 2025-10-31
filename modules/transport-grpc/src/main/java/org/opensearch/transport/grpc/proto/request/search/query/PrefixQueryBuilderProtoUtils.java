/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.protobufs.MultiTermQueryRewrite;
import org.opensearch.protobufs.PrefixQuery;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for converting PrefixQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of prefix queries
 * into their corresponding OpenSearch PrefixQueryBuilder implementations for search operations.
 */
class PrefixQueryBuilderProtoUtils {

    private PrefixQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer PrefixQuery to an OpenSearch PrefixQueryBuilder.
     * Similar to {@link PrefixQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * PrefixQueryBuilder with the appropriate field name, value, rewrite method, case insensitivity,
     * boost, and query name.
     *
     * @param prefixQueryProto The Protocol Buffer PrefixQuery object
     * @return A configured PrefixQueryBuilder instance
     * @throws IllegalArgumentException if the field name or value is null or empty
     */
    static PrefixQueryBuilder fromProto(PrefixQuery prefixQueryProto) {
        String fieldName = prefixQueryProto.getField();
        String value = prefixQueryProto.getValue();
        String rewrite = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean caseInsensitive = PrefixQueryBuilder.DEFAULT_CASE_INSENSITIVITY;

        if (prefixQueryProto.hasXName()) {
            queryName = prefixQueryProto.getXName();
        }

        if (prefixQueryProto.hasBoost()) {
            boost = prefixQueryProto.getBoost();
        }

        if (prefixQueryProto.hasRewrite()) {
            MultiTermQueryRewrite rewriteEnum = prefixQueryProto.getRewrite();
            if (rewriteEnum != MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_UNSPECIFIED) {
                rewrite = ProtobufEnumUtils.convertToString(rewriteEnum);
            }
        }
        if (prefixQueryProto.hasCaseInsensitive()) {
            caseInsensitive = prefixQueryProto.getCaseInsensitive();
        }

        return new PrefixQueryBuilder(fieldName, value).rewrite(rewrite).boost(boost).queryName(queryName).caseInsensitive(caseInsensitive);
    }
}
