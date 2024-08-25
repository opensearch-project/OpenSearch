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

import static org.opensearch.search.SearchHits.EMPTY;

/**
 * Serialization/Deserialization implementations for SearchHits.
 * @opensearch.internal
 */
public class SearchHitsSerDe implements SerDe.StreamSerializer<SearchHits>, SerDe.StreamDeserializer<SearchHits> {
    SearchHitSerDe searchHitSerDe;

    public SearchHitsSerDe () {
        this.searchHitSerDe = new SearchHitSerDe();
    }

    @Override
    public SearchHits deserialize(StreamInput in) {
        try {
            return fromStream(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(SearchHits object, StreamOutput out) throws SerDe.SerializationException {
        try {
            toStream(object, out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }

    SearchHitsProto toProto(SearchHits searchHits) throws SerDe.SerializationException{
        SearchHits.SerializationAccess serI = searchHits.getSerAccess();
        SearchHitsProto.Builder builder = SearchHitsProto.newBuilder()
            .setMaxScore(serI.getMaxScore())
            .setCollapseField(serI.getCollapseField());

        for (SearchHit hit : serI.getHits()) {
            builder.addHits(searchHitSerDe.toProto(hit));
        }

        TotalHits totHits = serI.getTotalHits();
        TotalHitsProto.Builder totHitsBuilder = TotalHitsProto.newBuilder()
            .setRelation(totHits.relation.ordinal())
            .setValue(totHits.value);
        builder.setTotalHits(totHitsBuilder);

        try (BytesStreamOutput sortOut = new BytesStreamOutput()) {
            sortOut.writeOptionalArray(Lucene::writeSortField, serI.getSortFields());
            builder.setSortFields(ByteString.copyFrom(sortOut.bytes().toBytesRef().bytes));
        } catch (IOException e){
            throw new SerDe.SerializationException("Failed to serialize SearchHits to proto", e);
        }

        try (BytesStreamOutput collapseOut = new BytesStreamOutput()) {
            collapseOut.writeOptionalArray(Lucene::writeSortField, serI.getSortFields());
            builder.setCollapseValues(ByteString.copyFrom(collapseOut.bytes().toBytesRef().bytes));
        } catch (IOException e){
            throw new SerDe.SerializationException("Failed to serialize SearchHits to proto", e);
        }

        return builder.build();
    }

    SearchHits fromProto(SearchHitsProto proto) throws SerDe.SerializationException {
        float maxScore = proto.getMaxScore();
        String collapseField = proto.getCollapseField();

        TotalHitsProto totalHits = proto.getTotalHits();
        long rel = totalHits.getRelation();
        long val = totalHits.getValue();
        if (rel < 0 || rel >= TotalHits.Relation.values().length) {
            throw new SerDe.SerializationException("Failed to deserialize TotalHits from proto");
        }
        TotalHits totHits = new TotalHits(val, TotalHits.Relation.values()[(int) rel]);

        SortField[] sortFields = null;
        try (StreamInput sortBytesInput = new BytesArray(proto.getSortFields().toByteArray()).streamInput()) {
            sortFields = sortBytesInput.readOptionalArray(Lucene::readSortField, SortField[]::new);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize SearchHits from proto", e);
        }

        Object[] collapseValues = null;
        try (StreamInput collapseBytesInput = new BytesArray(proto.getCollapseValues().toByteArray()).streamInput()) {
            collapseValues = collapseBytesInput.readOptionalArray(Lucene::readSortValue, Object[]::new);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize SearchHits from proto", e);
        }

        SearchHit[] hits = new SearchHit[proto.getHitsCount()];
        for(int i = 0; i < hits.length; i++) {
            hits[i] = searchHitSerDe.fromProto(proto.getHits(i));
        }

        return new SearchHits(hits, totHits, maxScore, sortFields, collapseField, collapseValues);
    }

    private SearchHits fromStream(StreamInput in) throws IOException {
        SearchHit[] hits;
        TotalHits totalHits;
        float maxScore;
        SortField[] sortFields;
        String collapseField;
        Object[] collapseValues;

        if (in.readBoolean()) {
            totalHits = Lucene.readTotalHits(in);
        } else {
            // track_total_hits is false
            totalHits = null;
        }
        maxScore = in.readFloat();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new SearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = searchHitSerDe.deserialize(in);
            }
        }
        sortFields = in.readOptionalArray(Lucene::readSortField, SortField[]::new);
        collapseField = in.readOptionalString();
        collapseValues = in.readOptionalArray(Lucene::readSortValue, Object[]::new);

        return new SearchHits(hits, totalHits, maxScore, sortFields, collapseField, collapseValues);
    }

    private void toStream(SearchHits object, StreamOutput out) throws IOException {
        SearchHits.SerializationAccess serI = object.getSerAccess();
        SearchHit[] hits = serI.getHits();
        TotalHits totalHits = serI.getTotalHits();
        float maxScore = serI.getMaxScore();
        SortField[] sortFields = serI.getSortFields();
        String collapseField = serI.getCollapseField();
        Object[] collapseValues = serI.getCollapseValues();

        final boolean hasTotalHits = totalHits != null;
        out.writeBoolean(hasTotalHits);
        if (hasTotalHits) {
            Lucene.writeTotalHits(out, totalHits);
        }
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            for (SearchHit hit : hits) {
                searchHitSerDe.serialize(hit, out);
            }
        }
        out.writeOptionalArray(Lucene::writeSortField, sortFields);
        out.writeOptionalString(collapseField);
        out.writeOptionalArray(Lucene::writeSortValue, collapseValues);
    }
}
