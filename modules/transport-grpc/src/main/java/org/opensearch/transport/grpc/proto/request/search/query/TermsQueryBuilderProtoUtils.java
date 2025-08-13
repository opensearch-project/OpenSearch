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
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.FieldValueArray;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.protobufs.TermsQueryValueType;
import org.opensearch.protobufs.ValueType;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Utility class for converting Terms query Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of terms queries
 * into their corresponding OpenSearch TermsQueryBuilder implementations for search operations.
 * Similar to {@link TermsQueryBuilder#fromXContent(XContentParser)}.
 */
public class TermsQueryBuilderProtoUtils {

    private TermsQueryBuilderProtoUtils() {

    }

    /**
     * Builds a TermsQueryBuilder from a field name, TermsQueryField oneof, and value_type.
     * @param fieldName the field name (from the terms map key)
     * @param termsQueryField the protobuf oneof (field_value_array or lookup)
     * @param valueTypeProto the container-level value_type
     * @return configured TermsQueryBuilder
     * @throws IllegalArgumentException if neither values nor lookup is set, or if bitmap validation fails
     */
    protected static TermsQueryBuilder fromProto(String fieldName, TermsQueryField termsQueryField, TermsQueryValueType valueTypeProto) {
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

        }

        if (values == null && termsLookup == null) {
            throw new IllegalArgumentException("Either field_value_array or lookup must be set");
        }

        TermsQueryBuilder.ValueType valueType = parseValueType(valueTypeProto);

        if (valueType == TermsQueryBuilder.ValueType.BITMAP) {
            if (values != null && values.size() == 1) {
                Object v = values.get(0);
                if (v instanceof BytesRef) {
                    byte[] decoded = Base64.getDecoder().decode(((BytesRef) v).utf8ToString());
                    values.set(0, new BytesArray(decoded));
                } else if (v instanceof String) {
                    byte[] decoded = Base64.getDecoder().decode((String) v);
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

    /**
     * Map protobuf {@link ValueType} to {@link TermsQueryBuilder.ValueType}.
     *
     * @param valueType the  protobuf value type to convert
     * @return the corresponding {@link TermsQueryBuilder.ValueType}
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
     * Map protobuf {@link TermsQueryValueType} to {@link TermsQueryBuilder.ValueType}.
     *
     * @param valueType the protobuf value type to convert
     * @return the corresponding {@link TermsQueryBuilder.ValueType}
     */
    public static TermsQueryBuilder.ValueType parseValueType(TermsQueryValueType valueType) {
        switch (valueType) {
            case TERMS_QUERY_VALUE_TYPE_BITMAP:
                return TermsQueryBuilder.ValueType.BITMAP;
            case TERMS_QUERY_VALUE_TYPE_DEFAULT:
            case TERMS_QUERY_VALUE_TYPE_UNSPECIFIED:
            default:
                return TermsQueryBuilder.ValueType.DEFAULT;
        }
    }

    /** Parse FieldValueArray into a List of Java values. */
    private static List<Object> parseFieldValueArray(FieldValueArray fieldValueArray) {
        List<Object> values = new ArrayList<>(fieldValueArray.getFieldValueArrayCount());
        for (FieldValue fv : fieldValueArray.getFieldValueArrayList()) {
            values.add(parseFieldValue(fv));
        }
        return values;
    }

    private static Object parseFieldValue(FieldValue fieldValue) {
        if (fieldValue.hasString()) {
            return fieldValue.getString();
        } else if (fieldValue.hasBool()) {
            return fieldValue.getBool();
        } else if (fieldValue.hasGeneralNumber()) {
            org.opensearch.protobufs.GeneralNumber number = fieldValue.getGeneralNumber();
            if (number.hasDoubleValue()) {
                return number.getDoubleValue();
            } else if (number.hasFloatValue()) {
                return number.getFloatValue();
            } else if (number.hasInt64Value()) {
                return number.getInt64Value();
            } else if (number.hasInt32Value()) {
                return number.getInt32Value();
            }
        } else if (fieldValue.hasNullValue()) {
            return null;
        }
        throw new IllegalArgumentException("Unsupported FieldValue variant");
    }
}
