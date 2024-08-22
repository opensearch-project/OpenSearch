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

public class FetchSearchResultsSerDe implements SerDe.StreamSerializer<FetchSearchResult>, SerDe.StreamDeserializer<FetchSearchResult> {
    /**
     * TODO NOTE: FetchSearchResult inheritance structure is as follows.
     * TransportMessage -> TransportResponse -> SearchPhaseResult -> FetchSearchResult.
     * Serialization of parent classes is currently a no-op.
     * For completeness these parent classes should be mirrored here respectively with:
     * TransportMessageSerDe, TransportResponseSerDe, SearchPhaseResultSerDe.
     * However, currently only SearchHitsSerDe is needed for serialization.
     *
     * This is implicitely enforced by FetchSearchResult as well on the serialization side.
     * writeTo doesn't call a parent implementation...
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
