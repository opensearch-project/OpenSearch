/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.serde;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchHitsProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.TotalHitsProto;

import java.io.IOException;

/**
 * Serialization/Deserialization implementations for SearchHits.
 * @opensearch.internal
 */
public class SearchHitsSerDe extends SearchHits implements SerDe.nativeSerializer, SerDe.protobufSerializer {
    SerDe.Strategy strategy = SerDe.Strategy.NATIVE;

    public SearchHitsSerDe(SearchHits hits, SerDe.Strategy strategy) {
        super(hits);
        this.strategy = strategy;
    }

    public SearchHitsSerDe(SerDe.Strategy strategy, StreamInput in) throws IOException {
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

    public SearchHitsSerDe(StreamInput in) throws IOException {
        fromNativeStream(in);
    }

    public SearchHitsSerDe(SearchHitsProto proto) {
        fromProto(proto);
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
        SearchHitsProto proto = SearchHitsProto.parseFrom(in);
        fromProto(proto);
    }

    @Override
    public void toNativeStream(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public void fromNativeStream(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.totalHits = Lucene.readTotalHits(in);
        } else {
            // track_total_hits is false
            this.totalHits = null;
        }
        this.maxScore = in.readFloat();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new SearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = new SearchHitSerDe(SerDe.Strategy.NATIVE, in);
            }
        }
        this.sortFields = in.readOptionalArray(Lucene::readSortField, SortField[]::new);
        this.collapseField = in.readOptionalString();
        this.collapseValues = in.readOptionalArray(Lucene::readSortValue, Object[]::new);
    }

    SearchHitsProto toProto() {
        SearchHitsProto.Builder builder = SearchHitsProto.newBuilder()
            .setMaxScore(maxScore)
            .setCollapseField(collapseField);

        for (SearchHit hit : hits) {
            builder.addHits(new SearchHitSerDe(hit, strategy).toProto());
        }

        TotalHits totHits = totalHits;
        TotalHitsProto.Builder totHitsBuilder = TotalHitsProto.newBuilder()
            .setRelation(totHits.relation.ordinal())
            .setValue(totHits.value);
        builder.setTotalHits(totHitsBuilder);

        try (BytesStreamOutput sortOut = new BytesStreamOutput()) {
            sortOut.writeOptionalArray(Lucene::writeSortField, sortFields);
            builder.setSortFields(ByteString.copyFrom(sortOut.bytes().toBytesRef().bytes));
        } catch (IOException e){
            throw new SerDe.SerializationException("Failed to serialize SearchHits to proto", e);
        }

        try (BytesStreamOutput collapseOut = new BytesStreamOutput()) {
            collapseOut.writeOptionalArray(Lucene::writeSortValue, collapseValues);
            builder.setCollapseValues(ByteString.copyFrom(collapseOut.bytes().toBytesRef().bytes));
        } catch (IOException e){
            throw new SerDe.SerializationException("Failed to serialize SearchHits to proto", e);
        }

        return builder.build();
    }

    void fromProto(SearchHitsProto proto) throws SerDe.SerializationException {
        maxScore = proto.getMaxScore();
        collapseField = proto.getCollapseField();

        TotalHitsProto totHitsProto = proto.getTotalHits();
        long rel = totHitsProto.getRelation();
        long val = totHitsProto.getValue();
        if (rel < 0 || rel >= TotalHits.Relation.values().length) {
            throw new SerDe.SerializationException("Failed to deserialize TotalHits from proto");
        }
        totalHits = new TotalHits(val, TotalHits.Relation.values()[(int) rel]);

        try (StreamInput sortBytesInput = new BytesArray(proto.getSortFields().toByteArray()).streamInput()) {
            sortFields = sortBytesInput.readOptionalArray(Lucene::readSortField, SortField[]::new);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize SearchHits from proto", e);
        }

        try (StreamInput collapseBytesInput = new BytesArray(proto.getCollapseValues().toByteArray()).streamInput()) {
            collapseValues = collapseBytesInput.readOptionalArray(Lucene::readSortValue, Object[]::new);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize SearchHits from proto", e);
        }

        hits = new SearchHit[proto.getHitsCount()];
        for(int i = 0; i < hits.length; i++) {
            try {
                hits[i] = new SearchHitSerDe(proto.getHits(i));
            } catch (IOException e) {
                throw new SerDe.SerializationException("Failed to deserialize SearchHits from proto", e);
            }
        }
    }
}
