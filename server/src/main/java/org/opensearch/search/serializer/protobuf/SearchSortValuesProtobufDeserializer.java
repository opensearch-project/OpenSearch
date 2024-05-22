/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.serializer.protobuf;

import org.opensearch.search.SearchSortValues;
import org.opensearch.search.serializer.SearchSortValuesDeserializer;
import org.opensearch.server.proto.FetchSearchResultProto;

import java.io.IOException;
import java.io.InputStream;

/**
 * Deserializer for {@link SearchSortValues} to/from protobuf.
 */
public class SearchSortValuesProtobufDeserializer implements SearchSortValuesDeserializer<InputStream> {

    @Override
    public SearchSortValues createSearchSortValues(InputStream inputStream) throws IOException {
        FetchSearchResultProto.SearchHit.SearchSortValues searchSortValues = FetchSearchResultProto.SearchHit.SearchSortValues.parseFrom(
            inputStream
        );
        Object[] formattedSortValues = new Object[searchSortValues.getFormattedSortValuesCount()];
        for (int i = 0; i < searchSortValues.getFormattedSortValuesCount(); i++) {
            formattedSortValues[i] = SearchHitsProtobufDeserializer.readSortValueFromProtobuf(searchSortValues.getFormattedSortValues(i));
        }
        Object[] rawSortValues = new Object[searchSortValues.getRawSortValuesCount()];
        for (int i = 0; i < searchSortValues.getRawSortValuesCount(); i++) {
            rawSortValues[i] = SearchHitsProtobufDeserializer.readSortValueFromProtobuf(searchSortValues.getRawSortValues(i));
        }
        return new SearchSortValues(formattedSortValues, rawSortValues);
    }

}
