/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import com.google.protobuf.ProtocolStringList;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.protobufs.TermsLookupField;
import org.opensearch.protobufs.TermsLookupFieldStringArrayMap;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.protobufs.ValueType;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.query.AbstractQueryBuilder.maybeConvertToBytesRef;

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

        String fieldName = null;
        List<Object> values = null;
        TermsLookup termsLookup = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String valueTypeStr = TermsQueryBuilder.ValueType.DEFAULT.name();

        if (termsQueryProto.hasBoost()) {
            boost = termsQueryProto.getBoost();
        }

        if (termsQueryProto.hasUnderscoreName()) {
            queryName = termsQueryProto.getUnderscoreName();
        }

        // TODO: remove this parameter when backporting to under OS 2.17
        if (termsQueryProto.hasValueType()) {
            valueTypeStr = parseValueType(termsQueryProto.getValueType()).name();
        }

        if (termsQueryProto.getTermsLookupFieldStringArrayMapMap().size() > 1) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query does not support more than one field. ");
        }

        for (Map.Entry<String, TermsLookupFieldStringArrayMap> entry : termsQueryProto.getTermsLookupFieldStringArrayMapMap().entrySet()) {
            fieldName = entry.getKey();
            TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = entry.getValue();

            if (termsLookupFieldStringArrayMap.hasTermsLookupField()) {
                TermsLookupField termsLookupField = termsLookupFieldStringArrayMap.getTermsLookupField();
                termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupField);
            } else if (termsLookupFieldStringArrayMap.hasStringArray()) {
                values = parseValues(termsLookupFieldStringArrayMap.getStringArray().getStringArrayList());
            } else {
                throw new IllegalArgumentException("termsLookupField and stringArray fields cannot both be null");
            }
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
     * Similar to {@link TermsQueryBuilder#parseValues(XContentParser)}
     * @param termsLookupFieldStringArray
     * @return
     * @throws IllegalArgumentException
     */
    static List<Object> parseValues(ProtocolStringList termsLookupFieldStringArray) throws IllegalArgumentException {
        List<Object> values = new ArrayList<>();

        for (Object value : termsLookupFieldStringArray) {
            Object convertedValue = maybeConvertToBytesRef(value);
            if (value == null) {
                throw new IllegalArgumentException("No value specified for terms query");
            }
            values.add(convertedValue);
        }
        return values;
    }
}
