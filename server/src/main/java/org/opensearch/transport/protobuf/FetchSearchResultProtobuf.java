/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobuf;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.proto.search.fetch.FetchSearchResultProtoDef.FetchSearchResultProto;

import java.io.IOException;

/**
 * FetchSearchResult which leverages protobuf for transport layer serialization.
 * @opensearch.internal
 */
public class FetchSearchResultProtobuf extends FetchSearchResult {
    public FetchSearchResultProtobuf(StreamInput in) throws IOException {
        fromProtobufStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        toProtobufStream(out);
    }

    public void toProtobufStream(StreamOutput out) throws IOException {
        toProto().writeTo(out);
    }

    public void fromProtobufStream(StreamInput in) throws IOException {
        FetchSearchResultProto proto = FetchSearchResultProto.parseFrom(in);
        fromProto(proto);
    }

    FetchSearchResultProto toProto() {
        FetchSearchResultProto.Builder builder = FetchSearchResultProto.newBuilder()
            .setHits(new SearchHitsProtobuf(hits).toProto())
            .setCounter(this.counter);
        return builder.build();
    }

    void fromProto(FetchSearchResultProto proto) {
        hits = new SearchHitsProtobuf(proto.getHits());
    }
}
