/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.serde;

import org.opensearch.search.SearchHits;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public class SearchHitsSerDe implements SerDe.StreamSerializer<SearchHits>, SerDe.StreamDeserializer<SearchHits> {
    SearchHitSerDe searchHitSerDe;

    @Override
    public SearchHits deserialize(StreamInput in) {
        try {
            return new SearchHits(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(SearchHits object, StreamOutput out) throws SerDe.SerializationException {
        try {
            object.writeTo(out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }
}
