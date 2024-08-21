/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.serde;

import org.opensearch.search.SearchHit;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public class SearchHitSerDe implements SerDe.StreamSerializer<SearchHit>, SerDe.StreamDeserializer<SearchHit> {

    @Override
    public SearchHit deserialize(StreamInput in) {
        try {
            return new SearchHit(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(SearchHit object, StreamOutput out) throws SerDe.SerializationException {
        try {
            object.writeTo(out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }
}
