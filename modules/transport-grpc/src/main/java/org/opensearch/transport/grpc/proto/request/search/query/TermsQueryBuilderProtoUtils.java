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
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.protobufs.ValueType;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Utility class for converting Terms query Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of terms queries
 * into their corresponding OpenSearch TermsQueryBuilder implementations for search operations.
 */
class TermsQueryBuilderProtoUtils {

    private TermsQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer TermsQuery to an OpenSearch TermQueryBuilder.
     * Similar to {@link TermsQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * TermQueryBuilder with the appropriate field name, values, boost, query name,
     * and value type settings.
     *
     * @param termsQueryProto The Protocol Buffer TermsQuery object
     * @return A configured TermQueryBuilder instance
     * @throws IllegalArgumentException if the terms query is invalid or missing required fields
     */
    static TermsQueryBuilder fromProto(org.opensearch.protobufs.TermsQuery termsQueryProto) {
        if (termsQueryProto == null) {
            throw new IllegalArgumentException("TermsQuery must not be null");
        }

        if (termsQueryProto.getTermsCount() != 1) {
            throw new IllegalArgumentException("TermsQuery must contain exactly one field, found: " + termsQueryProto.getTermsCount());
        }

        // Get the first entry from the map
        String fieldName = termsQueryProto.getTermsMap().keySet().iterator().next();
        org.opensearch.protobufs.TermsQueryField termsQueryField = termsQueryProto.getTermsMap().get(fieldName);

        // Get value type with default
        org.opensearch.protobufs.TermsQueryValueType vt = termsQueryProto.hasValueType()
            ? termsQueryProto.getValueType()
            : org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT;

        // Build the base TermsQueryBuilder
        TermsQueryBuilder builder = fromProto(fieldName, termsQueryField, vt);

        // Apply boost and queryName if provided
        if (termsQueryProto.hasBoost()) {
            builder.boost(termsQueryProto.getBoost());
        }
        if (termsQueryProto.hasXName()) {
            builder.queryName(termsQueryProto.getXName());
        }

        return builder;
    }

    /**
     * Converts a Protocol Buffer TermsQueryField to an OpenSearch TermQueryBuilder.
     * This method handles the field-specific conversion (values or lookup) without
     * boost, queryName, or valueType which are handled at the TermsQuery level.
     *
     * @param termsQueryProto The Protocol Buffer TermsQueryField object
     * @return A configured TermQueryBuilder instance
     * @throws IllegalArgumentException if the term query field value is not recognized
     */
    static TermsQueryBuilder fromProto(TermsQueryField termsQueryProto) {
        String fieldName = null;
        List<Object> values = null;
        TermsLookup termsLookup = null;

        switch (termsQueryProto.getTermsQueryFieldCase()) {
            case FIELD_VALUE_ARRAY:
                values = parseFieldValueArray(termsQueryProto.getFieldValueArray());
                break;
            case LOOKUP:
                termsLookup = parseTermsLookup(termsQueryProto.getLookup());
                break;
            case TERMSQUERYFIELD_NOT_SET:
            default:
                throw new IllegalArgumentException("Neither field_value_array nor lookup is set");
        }

        if (values == null && termsLookup == null) {
            throw new IllegalArgumentException("Either field_value_array or lookup must be set");
        }

        TermsQueryBuilder termsQueryBuilder;
        if (values == null) {
            termsQueryBuilder = new TermsQueryBuilder(fieldName, termsLookup);
        } else if (termsLookup == null) {
            termsQueryBuilder = new TermsQueryBuilder(fieldName, values);
        } else {
            throw new IllegalArgumentException("values and termsLookup cannot both be null");
        }

        return termsQueryBuilder;
    }

    /**
     * Builds a TermsQueryBuilder from a field name, TermsQueryField oneof, and value_type.
     * @param fieldName the field name (from the terms map key)
     * @param termsQueryField the protobuf oneof (field_value_array or lookup)
     * @param valueTypeProto the container-level value_type
     * @return configured TermsQueryBuilder
     * @throws IllegalArgumentException if neither values nor lookup is set, or if bitmap validation fails
     */
    static TermsQueryBuilder fromProto(
        String fieldName,
        org.opensearch.protobufs.TermsQueryField termsQueryField,
        org.opensearch.protobufs.TermsQueryValueType valueTypeProto
    ) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("fieldName must be provided");
        }

        List<Object> values = null;
        TermsLookup termsLookup = null;

        switch (termsQueryField.getTermsQueryFieldCase()) {
            case FIELD_VALUE_ARRAY:
                values = parseFieldValueArray(termsQueryField.getFieldValueArray());
                break;
            case LOOKUP:
                termsLookup = parseTermsLookup(termsQueryField.getLookup());
                break;
            case TERMSQUERYFIELD_NOT_SET:
            default:
                throw new IllegalArgumentException("Neither field_value_array nor lookup is set");
        }

        if (values == null && termsLookup == null) {
            throw new IllegalArgumentException("Either field_value_array or lookup must be set");
        }

        TermsQueryBuilder.ValueType valueType = parseValueType(valueTypeProto);

        if (valueType == TermsQueryBuilder.ValueType.BITMAP) {
            if (values != null && values.size() == 1) {
                Object v = values.get(0);
                if (v instanceof BytesRef bytesRef) {
                    byte[] decoded = Base64.getDecoder().decode(bytesRef.utf8ToString());
                    values.set(0, new BytesArray(decoded));
                } else if (v instanceof String string) {
                    byte[] decoded = Base64.getDecoder().decode(string);
                    values.set(0, new BytesArray(decoded));
                } else {
                    throw new IllegalArgumentException("Invalid value for bitmap type");
                }
            } else if (termsLookup == null) {
                throw new IllegalArgumentException("Bitmap type requires a single base64 value or a lookup");
            }
        }

        TermsQueryBuilder termsQueryBuilder = (values != null)
            ? new TermsQueryBuilder(fieldName, values)
            : new TermsQueryBuilder(fieldName, termsLookup);

        return termsQueryBuilder.valueType(valueType);
    }

    /**
     * Parses a protobuf ScriptLanguage to a String representation
     *
     * See {@link org.opensearch.index.query.TermsQueryBuilder.ValueType#fromString(String)}  }
     * *
     * @param valueType the Protocol Buffer ValueType to convert
     * @return the string representation of the script language
     * @throws UnsupportedOperationException if no language was specified
     */
    public static TermsQueryBuilder.ValueType parseValueType(ValueType valueType) {
        switch (valueType) {
            case VALUE_TYPE_BITMAP:
                return TermsQueryBuilder.ValueType.BITMAP;
            case VALUE_TYPE_DEFAULT:
                return TermsQueryBuilder.ValueType.DEFAULT;
            case VALUE_TYPE_UNSPECIFIED:
            default:
                return TermsQueryBuilder.ValueType.DEFAULT;
        }
    }

    /**
     * Parses a protobuf TermsQueryValueType to OpenSearch TermsQueryBuilder.ValueType
     * @param valueTypeProto the Protocol Buffer TermsQueryValueType to convert
     * @return the OpenSearch TermsQueryBuilder.ValueType
     */
    protected static TermsQueryBuilder.ValueType parseValueType(org.opensearch.protobufs.TermsQueryValueType valueTypeProto) {
        switch (valueTypeProto) {
            case TERMS_QUERY_VALUE_TYPE_BITMAP:
                return TermsQueryBuilder.ValueType.BITMAP;
            case TERMS_QUERY_VALUE_TYPE_DEFAULT:
            case TERMS_QUERY_VALUE_TYPE_UNSPECIFIED:
            default:
                return TermsQueryBuilder.ValueType.DEFAULT;
        }
    }

    /**
     * Parses a protobuf FieldValueArray to a List of Objects
     * @param fieldValueArray the Protocol Buffer FieldValueArray to convert
     * @return List of parsed Objects
     */
    private static List<Object> parseFieldValueArray(org.opensearch.protobufs.FieldValueArray fieldValueArray) {
        if (fieldValueArray == null) {
            return null;
        }

        List<Object> values = new ArrayList<>();
        for (org.opensearch.protobufs.FieldValue fieldValue : fieldValueArray.getFieldValueArrayList()) {
            Object convertedValue = FieldValueProtoUtils.fromProto(fieldValue);
            if (convertedValue == null) {
                throw new IllegalArgumentException("No value specified for terms query");
            }
            values.add(convertedValue);
        }
        return values;
    }

    /**
     * Parses a protobuf TermsLookup to OpenSearch TermsLookup
     * @param lookup the Protocol Buffer TermsLookup to convert
     * @return OpenSearch TermsLookup
     */
    private static TermsLookup parseTermsLookup(org.opensearch.protobufs.TermsLookup lookup) {
        if (lookup == null) {
            return null;
        }
        TermsLookup tl = new TermsLookup(lookup.getIndex(), lookup.getId(), lookup.getPath());
        if (lookup.hasRouting()) {
            tl.routing(lookup.getRouting());
        }
        if (lookup.hasStore()) {
            tl.store(lookup.getStore());
        }
        return tl;
    }
}
