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
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.protobufs.WildcardQuery;

/**
 * Utility class for converting wildcardQueryProto Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of wildcard queries
 * into their corresponding OpenSearch WildcardQueryBuilder implementations for search operations.
 */
class WildcardQueryBuilderProtoUtils {

    private WildcardQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer wildcardQueryProto map to an OpenSearch WildcardQueryBuilder.
     * Similar to {@link WildcardQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * WildcardQueryBuilder with the appropriate field name, value, boost, query name,
     * rewrite method, and case sensitivity settings.
     *
     * @param wildcardQueryProto The map of field names to Protocol Buffer wildcardQueryProto objects
     * @return A configured WildcardQueryBuilder instance
     * @throws IllegalArgumentException if neither value nor wildcard field is set
     */
    static WildcardQueryBuilder fromProto(WildcardQuery wildcardQueryProto) {
        String fieldName = wildcardQueryProto.getField();
        String rewrite = null;
        String value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean caseInsensitive = WildcardQueryBuilder.DEFAULT_CASE_INSENSITIVITY;
        String queryName = null;

        if (wildcardQueryProto.hasValue()) {
            value = wildcardQueryProto.getValue();
        } else if (wildcardQueryProto.hasWildcard()) {
            value = wildcardQueryProto.getWildcard();
        } else {
            throw new IllegalArgumentException("Either value or wildcard field must be set in wildcardQueryProto");
        }

        // Process parameters in the exact same order as fromXContent
        if (wildcardQueryProto.hasBoost()) {
            boost = wildcardQueryProto.getBoost();
        }

        if (wildcardQueryProto.hasRewrite()) {
            rewrite = wildcardQueryProto.getRewrite();
        }

        if (wildcardQueryProto.hasCaseInsensitive()) {
            caseInsensitive = wildcardQueryProto.getCaseInsensitive();
        }

        if (wildcardQueryProto.hasXName()) {
            queryName = wildcardQueryProto.getXName();
        }

        return new WildcardQueryBuilder(fieldName, value).rewrite(rewrite)
            .boost(boost)
            .queryName(queryName)
            .caseInsensitive(caseInsensitive);
    }
}
