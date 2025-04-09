/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeneralNumber;
import org.opensearch.search.searchafter.SearchAfterBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting SearchAfterBuilder Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of search_after
 * values into their corresponding OpenSearch object arrays for pagination in search operations.
 */
public class SearchAfterBuilderProtoUtils {

    private SearchAfterBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a list of Protocol Buffer FieldValue objects to an array of Java objects
     * that can be used for search_after pagination.
     * Similar to {@link SearchAfterBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates an array of values
     * that can be used for search_after pagination.
     *
     * @param searchAfterProto The list of Protocol Buffer FieldValue objects to convert
     * @return An array of Java objects representing the search_after values
     * @throws IOException if there's an error during parsing or conversion
     */
    protected static Object[] fromProto(List<FieldValue> searchAfterProto) throws IOException {
        List<Object> values = new ArrayList<>();

        for (FieldValue fieldValue : searchAfterProto) {
            if (fieldValue.hasGeneralNumber()) {
                GeneralNumber generalNumber = fieldValue.getGeneralNumber();
                if (generalNumber.hasInt32Value()) {
                    values.add(generalNumber.getInt32Value());
                } else if (generalNumber.hasInt64Value()) {
                    values.add(generalNumber.getInt64Value());
                } else if (generalNumber.hasDoubleValue()) {
                    values.add(generalNumber.getDoubleValue());
                } else if (generalNumber.hasFloatValue()) {
                    values.add(generalNumber.getFloatValue());
                }
            } else if (fieldValue.hasStringValue()) {
                values.add(fieldValue.getStringValue());
            } else if (fieldValue.hasBoolValue()) {
                values.add(fieldValue.getBoolValue());
            }
            // TODO missing null value
            // else if(fieldValue.hasNullValue ()){
            // values.add(fieldValue.getNullValue());
            // }
        }
        return values.toArray();
    }

}
