/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.serde;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.fetch.FetchSearchResult;

import java.io.IOException;

public class FetchSearchResultsSerDe implements SerDe.StreamSerializer<FetchSearchResult>, SerDe.StreamDeserializer<FetchSearchResult> {
    SearchHitsSerDe searchHitsSerDe;

    @Override
    public FetchSearchResult deserialize(StreamInput in) {
        try {
            return new FetchSearchResult(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(FetchSearchResult object, StreamOutput out) throws SerDe.SerializationException {
        try {
            object.writeTo(out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }
}
