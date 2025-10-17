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
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

/**
 * Utility class for converting TermQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of term queries
 * into their corresponding OpenSearch TermQueryBuilder implementations for search operations.
 */
class TermQueryBuilderProtoUtils {

    private TermQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer TermQuery to an OpenSearch TermQueryBuilder.
     * Similar to {@link TermQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * TermQueryBuilder with the appropriate field name, value, boost, query name,
     * and case sensitivity settings.
     *
     * @param termQueryProto The Protocol Buffer TermQuery object
     * @return A configured TermQueryBuilder instance
     * @throws IllegalArgumentException if the field value type is not supported, or if the term query field value is not recognized
     */
    static TermQueryBuilder fromProto(TermQuery termQueryProto) {
        String queryName = null;
        String fieldName = termQueryProto.getField();
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean caseInsensitive = TermQueryBuilder.DEFAULT_CASE_INSENSITIVITY;

        if (termQueryProto.hasXName()) {
            queryName = termQueryProto.getXName();
        }
        if (termQueryProto.hasBoost()) {
            boost = termQueryProto.getBoost();
        }

        FieldValue fieldValue = termQueryProto.getValue();
        value = FieldValueProtoUtils.fromProto(fieldValue, false);

        if (termQueryProto.hasCaseInsensitive()) {
            caseInsensitive = termQueryProto.getCaseInsensitive();
        }

        TermQueryBuilder termQuery = new TermQueryBuilder(fieldName, value);
        termQuery.boost(boost);
        if (queryName != null) {
            termQuery.queryName(queryName);
        }
        termQuery.caseInsensitive(caseInsensitive);

        return termQuery;
    }

}
