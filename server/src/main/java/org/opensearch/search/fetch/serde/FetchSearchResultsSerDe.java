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
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;

import java.io.IOException;

/**
 * Serialization/Deserialization implementations for SearchHit.
 * @opensearch.internal
 */
public class FetchSearchResultsSerDe implements SerDe.StreamSerializer<FetchSearchResult>, SerDe.StreamDeserializer<FetchSearchResult> {
    /**
     * NOTE: FetchSearchResultSerDe should mirror class inheritence strucutre.
     * TransportMessage -> TransportResponse -> SearchPhaseResult -> FetchSearchResult.
     *
     * Serialization of parent classes is currently a no-op so leaving as is for now.
     */
    SearchHitsSerDe searchHitsSerDe;

    @Override
    public FetchSearchResult deserialize(StreamInput in) {
        try {
            return fromStream(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(FetchSearchResult object, StreamOutput out) throws SerDe.SerializationException {
        try {
            toStream(object, out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }

    private FetchSearchResult fromStream(StreamInput in) throws IOException {
        ShardSearchContextId contextId = new ShardSearchContextId(in);
        SearchHits hits = new SearchHits(in);
        return new FetchSearchResult(contextId, hits);
    }

    private void toStream(FetchSearchResult object, StreamOutput out) throws IOException {
        FetchSearchResult.SerializationAccess serI = object.getSerAccess();
        ShardSearchContextId contextId = serI.getShardSearchContextId();
        SearchHits hits = serI.getHits();

        contextId.writeTo(out);
        hits.writeTo(out);
    }
}
