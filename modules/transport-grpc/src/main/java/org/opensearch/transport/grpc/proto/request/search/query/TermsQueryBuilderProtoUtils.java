/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.protobufs.TermsQueryField;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Utility class for converting TermQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of term queries
 * into their corresponding OpenSearch TermQueryBuilder implementations for search operations.
 */
public class TermsQueryBuilderProtoUtils {

    private TermsQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer TermQuery map to an OpenSearch TermQueryBuilder.
     * Similar to {@link TermsQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * TermQueryBuilder with the appropriate field name, value, boost, query name,
     * and case sensitivity settings.
     *
     * @param termsQueryProto The map of field names to Protocol Buffer TermsQuery objects
     * @return A configured TermQueryBuilder instance
     * @throws IllegalArgumentException if the term query map has more than one element,
     *         if the field value type is not supported, or if the term query field value is not recognized
     */
    protected static TermsQueryBuilder fromProto(TermsQueryField termsQueryProto) {
        if (termsQueryProto == null) {
            throw new IllegalArgumentException("TermsQueryField cannot be null");
        }

        String fieldName = "simplified_field"; // Keep the original field name for compatibility
        List<Object> values = null;
        TermsLookup termsLookup = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String valueTypeStr = TermsQueryBuilder.ValueType.DEFAULT.name();

        // Handle the new protobuf 0.8.0 structure
        switch (termsQueryProto.getTermsQueryFieldCase()) {
            case FIELD_VALUE_ARRAY:
                if (termsQueryProto.hasFieldValueArray()) {
                    values = parseFieldValueArray(termsQueryProto.getFieldValueArray());
                }
                break;
            case LOOKUP:
                if (termsQueryProto.hasLookup()) {
                    termsLookup = parseTermsLookup(termsQueryProto.getLookup());
                }
                break;
            case TERMSQUERYFIELD_NOT_SET:
            default:
                // No values or lookup set
                break;
        }

        // Ensure we have either values or termsLookup
        if (values == null && termsLookup == null) {
            // Create empty values list to avoid TermsQueryBuilder constructor error
            values = new ArrayList<>();
        }

        TermsQueryBuilder.ValueType valueType = TermsQueryBuilder.ValueType.fromString(valueTypeStr);

        if (valueType == TermsQueryBuilder.ValueType.BITMAP) {
            if (values != null && values.size() == 1 && values.get(0) instanceof BytesRef) {
                values.set(0, new BytesArray(Base64.getDecoder().decode(((BytesRef) values.get(0)).utf8ToString())));
            } else if (termsLookup == null) {
                throw new IllegalArgumentException(
                    "Invalid value for bitmap type: Expected a single-element array with a base64 encoded serialized bitmap."
                );
            }
        }

        TermsQueryBuilder termsQueryBuilder;
        if (values == null) {
            termsQueryBuilder = new TermsQueryBuilder(fieldName, termsLookup);
        } else if (termsLookup == null) {
            termsQueryBuilder = new TermsQueryBuilder(fieldName, values);
        } else {
            throw new IllegalArgumentException("values and termsLookup cannot both be null");
        }

        return termsQueryBuilder.boost(boost).queryName(queryName).valueType(valueType);
    }

    /**
     * Parse FieldValueArray to extract values
     */
    private static List<Object> parseFieldValueArray(org.opensearch.protobufs.FieldValueArray fieldValueArray) {
        List<Object> values = new ArrayList<>();

        for (int i = 0; i < fieldValueArray.getFieldValueArrayCount(); i++) {
            org.opensearch.protobufs.FieldValue fieldValue = fieldValueArray.getFieldValueArray(i);
            Object value = parseFieldValue(fieldValue);
            if (value != null) {
                values.add(value);
            }
        }

        return values;
    }

    /**
     * Parse individual FieldValue
     */
    private static Object parseFieldValue(org.opensearch.protobufs.FieldValue fieldValue) {
        if (fieldValue.hasString()) {
            return fieldValue.getString();
        } else if (fieldValue.hasBool()) {
            return fieldValue.getBool();
        } else if (fieldValue.hasFloat()) {
            return fieldValue.getFloat();
        } else if (fieldValue.hasNullValue()) {
            return null;
        }
        return null;
    }

    /**
     * Parse TermsLookup from protobuf
     */
    private static TermsLookup parseTermsLookup(org.opensearch.protobufs.TermsLookup lookup) {
        return new TermsLookup(lookup.getIndex(), lookup.getId(), lookup.getPath());
    }
}
