/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobuf;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.proto.search.SearchHitsProtoDef.SearchHitsProto;
import org.opensearch.proto.search.SearchHitsProtoDef.TotalHitsProto;
import org.opensearch.proto.search.SearchHitsProtoDef.SortFieldProto;
import org.opensearch.proto.search.SearchHitsProtoDef.SortValueProto;
import org.opensearch.transport.TransportSerializationException;

import java.io.IOException;
import java.util.List;

import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortValueToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortValueFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortFieldFromProto;

/**
 * SearchHits child which implements serde operations as protobuf.
 * @opensearch.internal
 */
public class SearchHitsProtobuf extends SearchHits {
    public SearchHitsProtobuf(SearchHits hits) {
        super(hits);
    }

    public SearchHitsProtobuf(StreamInput in) throws IOException {
        fromProtobufStream(in);
    }

    public SearchHitsProtobuf(SearchHitsProto proto) {
        fromProto(proto);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        toProtobufStream(out);
    }

    public void toProtobufStream(StreamOutput out) throws IOException {
        toProto().writeTo(out);
    }

    public void fromProtobufStream(StreamInput in) throws IOException {
        SearchHitsProto proto = SearchHitsProto.parseFrom(in);
        fromProto(proto);
    }

    SearchHitsProto toProto() {
        SearchHitsProto.Builder builder = SearchHitsProto.newBuilder().setMaxScore(maxScore);

        for (SearchHit hit : hits) {
            builder.addHits(new SearchHitProtobuf(hit).toProto());
        }

        if (collapseField != null) {
            builder.setCollapseField(collapseField);
        }

        if (totalHits != null) {
            TotalHitsProto.Builder totHitsBuilder = TotalHitsProto.newBuilder()
                .setRelation(totalHits.relation.ordinal())
                .setValue(totalHits.value);
            builder.setTotalHits(totHitsBuilder);
        }

        if (sortFields != null) {
            for (SortField field : sortFields) {
                builder.addSortFields(sortFieldToProto(field));
            }
        }

        if (collapseValues != null) {
            for (Object col : collapseValues) {
                builder.addCollapseValues(sortValueToProto(col));
            }
        }

        return builder.build();
    }

    void fromProto(SearchHitsProto proto) throws TransportSerializationException {
        maxScore = proto.getMaxScore();

        hits = new SearchHit[proto.getHitsCount()];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = new SearchHitProtobuf(proto.getHits(i));
        }

        collapseField = proto.hasCollapseField()? proto.getCollapseField() : null;
        totalHits = proto.hasTotalHits()? totalHitsFromProto(proto.getTotalHits()) : null;
        sortFields = proto.getSortFieldsCount() > 0? sortFieldsFromProto(proto.getSortFieldsList()) : null;
        collapseValues = proto.getCollapseValuesCount() > 0? collapseValuesFromProto(proto.getCollapseValuesList()) : null;
    }

    private TotalHits totalHitsFromProto(TotalHitsProto proto) {
        long rel = proto.getRelation();
        long val = proto.getValue();
        if (rel < 0 || rel >= TotalHits.Relation.values().length) {
            throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize TotalHits from proto");
        }
        return new TotalHits(val, TotalHits.Relation.values()[(int) rel]);
    }

    private SortField[] sortFieldsFromProto(List<SortFieldProto> protoList) {
        SortField[] fields = new SortField[protoList.size()];
        for (int i = 0; i < protoList.size(); i++) {
            fields[i] = sortFieldFromProto(protoList.get(i));
        }
        return fields;
    }

    private Object[] collapseValuesFromProto(List<SortValueProto> protoList) {
        Object[] vals = new Object[protoList.size()];
        for (int i = 0; i < protoList.size(); i++) {
            vals[i] = sortValueFromProto(protoList.get(i));
        }
        return vals;
    }
}
