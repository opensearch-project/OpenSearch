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

import java.io.IOException;

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

    void fromProto(SearchHitsProto proto) throws ProtoSerDeHelpers.SerializationException {
        maxScore = proto.getMaxScore();

        hits = new SearchHit[proto.getHitsCount()];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = new SearchHitProtobuf(proto.getHits(i));
        }

        collapseField = proto.hasCollapseField()? proto.getCollapseField() : null;

        if (proto.hasTotalHits()) {
            long rel = proto.getTotalHits().getRelation();
            long val = proto.getTotalHits().getValue();
            if (rel < 0 || rel >= TotalHits.Relation.values().length) {
                throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize TotalHits from proto");
            }
            totalHits = new TotalHits(val, TotalHits.Relation.values()[(int) rel]);
        } else {
            totalHits = null;
        }

        if (proto.getSortFieldsCount() > 0) {
            sortFields = new SortField[proto.getSortFieldsCount()];
            for (int i = 0; i < proto.getSortFieldsCount(); i++) {
                SortFieldProto field = proto.getSortFields(i);
                sortFields[i] = sortFieldFromProto(field);
            }
        } else {
            sortFields = null;
        }

        if (proto.getCollapseValuesCount() > 0) {
            collapseValues = new Object[proto.getCollapseValuesCount()];
            for (int i = 0; i < proto.getCollapseValuesCount(); i++) {
                SortValueProto val = proto.getCollapseValues(i);
                collapseValues[i] = sortValueFromProto(val);
            }
        } else {
            collapseValues = null;
        }
    }
}
