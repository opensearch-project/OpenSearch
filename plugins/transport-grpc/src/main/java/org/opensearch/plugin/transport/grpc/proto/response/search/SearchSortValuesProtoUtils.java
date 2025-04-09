/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.search;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.protobufs.Hit;
import org.opensearch.search.SearchSortValues;

/**
 * Utility class for converting SearchSortVaues objects to Protocol Buffers.
 * This class handles the conversion of document get operation results to their
 * Protocol Buffer representation.
 */
public class SearchSortValuesProtoUtils {

    private SearchSortValuesProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SearchSortVaues values (list of objects) to its Protocol Buffer representation.
     * This method is equivalent to the  {@link SearchSortValues#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hitBuilder the Hit builder to populate with sort values
     * @param sortValues the array of sort values to convert
     */

    protected static void toProto(Hit.Builder hitBuilder, Object[] sortValues) {
        for (Object sortValue : sortValues) {
            hitBuilder.addSort(FieldValueProtoUtils.toProto(sortValue));
        }
    }
}
