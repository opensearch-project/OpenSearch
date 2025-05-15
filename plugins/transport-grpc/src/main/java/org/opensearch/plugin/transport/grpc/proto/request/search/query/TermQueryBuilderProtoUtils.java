/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugin.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.TermQuery;

import java.util.Map;

/**
 * Utility class for converting TermQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of term queries
 * into their corresponding OpenSearch TermQueryBuilder implementations for search operations.
 */
public class TermQueryBuilderProtoUtils {

    private TermQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer TermQuery map to an OpenSearch TermQueryBuilder.
     * Similar to {@link TermQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * TermQueryBuilder with the appropriate field name, value, boost, query name,
     * and case sensitivity settings.
     *
     * @param termQueryProto The map of field names to Protocol Buffer TermQuery objects
     * @return A configured TermQueryBuilder instance
     * @throws IllegalArgumentException if the term query map has more than one element,
     *         if the field value type is not supported, or if the term query field value is not recognized
     */
    protected static TermQueryBuilder fromProto(Map<String, TermQuery> termQueryProto) {
        String queryName = null;
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean caseInsensitive = TermQueryBuilder.DEFAULT_CASE_INSENSITIVITY;

        if (termQueryProto.size() > 1) {
            throw new IllegalArgumentException("Term query can only have 1 element in the map");
        }

        for (Map.Entry<String, TermQuery> entry : termQueryProto.entrySet()) {

            fieldName = entry.getKey();

            TermQuery termQuery = entry.getValue();

            if (termQuery.hasName()) {
                queryName = termQuery.getName();
            }
            if (termQuery.hasBoost()) {
                boost = termQuery.getBoost();
            }

            FieldValue fieldValue = termQuery.getValue();

            switch (fieldValue.getTypeCase()) {
                case GENERAL_NUMBER:
                    switch (fieldValue.getGeneralNumber().getValueCase()) {
                        case INT32_VALUE:
                            value = fieldValue.getGeneralNumber().getInt32Value();
                            break;
                        case INT64_VALUE:
                            value = fieldValue.getGeneralNumber().getInt64Value();
                            break;
                        case FLOAT_VALUE:
                            value = fieldValue.getGeneralNumber().getFloatValue();
                            break;
                        case DOUBLE_VALUE:
                            value = fieldValue.getGeneralNumber().getDoubleValue();
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "Unsupported general nunber type: " + fieldValue.getGeneralNumber().getValueCase()
                            );
                    }
                    break;
                case STRING_VALUE:
                    value = fieldValue.getStringValue();
                    break;
                case OBJECT_MAP:
                    value = ObjectMapProtoUtils.fromProto(fieldValue.getObjectMap());
                    break;
                case BOOL_VALUE:
                    value = fieldValue.getBoolValue();
                    break;
                default:
                    throw new IllegalArgumentException("TermQuery field value not recognized");
            }

            if (termQuery.hasCaseInsensitive()) {
                caseInsensitive = termQuery.getCaseInsensitive();
            }

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
