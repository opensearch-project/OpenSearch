/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.serde;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.serde.proto.SearchHitsTransportProto.FetchSearchResultProto;

import java.io.IOException;

/**
 * Serialization/Deserialization implementations for SearchHit.
 * @opensearch.internal
 */
public class FetchSearchResultSerDe extends FetchSearchResult implements SerDe.nativeSerializer, SerDe.protobufSerializer {
    SerDe.Strategy strategy = SerDe.Strategy.NATIVE;

    public FetchSearchResultSerDe(SerDe.Strategy strategy, StreamInput in) throws IOException {
        this.strategy = strategy;
        switch (this.strategy) {
            case NATIVE:
                fromNativeStream(in);
                break;
            case PROTOBUF:
                fromProtobufStream(in);
                break;
            default:
                throw new AssertionError("This code should not be reachable");
        }
    }

    public FetchSearchResultSerDe(StreamInput in) throws IOException {
        fromNativeStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        switch (this.strategy) {
            case NATIVE:
                toNativeStream(out);
                break;
            case PROTOBUF:
                toProtobufStream(out);
                break;
            default:
                throw new AssertionError("This code should not be reachable");
        }
    }

    @Override
    public void toProtobufStream(StreamOutput out) throws IOException {
        toProto().writeTo(out);
    }

    @Override
    public void fromProtobufStream(StreamInput in) throws IOException {
        FetchSearchResultProto proto = FetchSearchResultProto.parseFrom(in);
        fromProto(proto);
    }

    @Override
    public void toNativeStream(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public void fromNativeStream(StreamInput in) throws IOException {
        this.hits = new SearchHitsSerDe(SerDe.Strategy.NATIVE, in);
        this.contextId = new ShardSearchContextId(in);
    }

    FetchSearchResultProto toProto() {
        FetchSearchResultProto.Builder builder = FetchSearchResultProto.newBuilder()
            .setHits(new SearchHitsSerDe(hits, strategy).toProto())
            .setCounter(this.counter);
        return builder.build();
    }

    void fromProto(FetchSearchResultProto proto) {
        hits = new SearchHitsSerDe(proto.getHits());
    }
}
