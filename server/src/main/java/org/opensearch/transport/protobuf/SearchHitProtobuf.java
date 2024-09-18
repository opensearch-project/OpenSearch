/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobuf;

import com.google.protobuf.ByteString;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.text.Text;
import org.opensearch.search.SearchHit;
import org.opensearch.proto.search.SearchHitsProtoDef.SearchHitsProto;
import org.opensearch.proto.search.SearchHitsProtoDef.SearchHitProto;
import org.opensearch.proto.search.SearchHitsProtoDef.NestedIdentityProto;
import org.opensearch.proto.search.SearchHitsProtoDef.DocumentFieldProto;
import org.opensearch.proto.search.SearchHitsProtoDef.HighlightFieldProto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchSortValuesFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchSortValuesToProto;

/**
 * Serialization/Deserialization implementations for SearchHit.
 * @opensearch.internal
 */
public class SearchHitProtobuf extends SearchHit {
    public SearchHitProtobuf(SearchHit hit) {
        super(hit);
    }

    public SearchHitProtobuf(StreamInput in) throws IOException {
        fromProtobufStream(in);
    }

    public SearchHitProtobuf(SearchHitProto proto) {
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
        SearchHitProto proto = SearchHitProto.parseFrom(in);
        fromProto(proto);
    }

    SearchHitProto toProto() {
        SearchHitProto.Builder builder = SearchHitProto.newBuilder()
            .setScore(score)
            .setVersion(version)
            .setSeqNo(seqNo)
            .setPrimaryTerm(primaryTerm);

        if (id != null) {
            builder.setId(id.string());
        }

        if (nestedIdentity != null) {
            builder.setNestedIdentity(nestedIdentityToProto(nestedIdentity));
        }

        if (source != null) {
            builder.setSource(ByteString.copyFrom(source.toBytesRef().bytes));
        }

        if (explanation != null) {
            builder.setExplanation(explanationToProto(explanation));
        }

        builder.setSortValues(searchSortValuesToProto(sortValues));

        documentFields.forEach((key, value) -> builder.putDocumentFields(key, documentFieldToProto(value)));

        metaFields.forEach((key, value) -> builder.putMetaFields(key, documentFieldToProto(value)));

        if (highlightFields != null) {
            highlightFields.forEach((key, value) -> builder.putHighlightFields(key, highlightFieldToProto(value)));
        }

        matchedQueries.forEach(builder::putMatchedQueries);

        if (shard != null) {
            builder.setShard(searchShardTargetToProto(shard));
        }

        if (innerHits != null) {
            innerHits.forEach((key, value) -> builder.putInnerHits(key, new SearchHitsProtobuf(value).toProto()));
        }

        return builder.build();
    }

    void fromProto(SearchHitProto proto) {
        docId = -1;
        score = proto.getScore();
        seqNo = proto.getSeqNo();
        version = proto.getVersion();
        primaryTerm = proto.getPrimaryTerm();
        sortValues = searchSortValuesFromProto(proto.getSortValues());
        matchedQueries = proto.getMatchedQueriesMap();

        if (proto.hasId()) {
            id = new Text(proto.getId());
        } else {
            id = null;
        }

        if (proto.hasNestedIdentity()) {
            nestedIdentity = nestedIdentityFromProto(proto.getNestedIdentity());
        } else {
            nestedIdentity = null;
        }

        if (proto.hasSource()) {
            source = BytesReference.fromByteBuffer(proto.getSource().asReadOnlyByteBuffer());
        } else {
            source = null;
        }

        if (proto.hasExplanation()) {
            explanation = explanationFromProto(proto.getExplanation());
        } else {
            explanation = null;
        }

        if (proto.hasShard()) {
            shard = searchShardTargetFromProto(proto.getShard());
            index = shard.getIndex();
            clusterAlias = shard.getClusterAlias();
        } else {
            shard = null;
            index = null;
            clusterAlias = null;
        }

        Map<String, SearchHitsProto> innerHitsProto = proto.getInnerHitsMap();
        if (!innerHitsProto.isEmpty()) {
            innerHits = new HashMap<>();
            innerHitsProto.forEach((key, value) -> innerHits.put(key, new SearchHitsProtobuf(value)));
        }

        documentFields = new HashMap<>();
        Map<String, DocumentFieldProto> documentFieldProtoMap = proto.getDocumentFieldsMap();
        if (!documentFieldProtoMap.isEmpty()) {
            documentFieldProtoMap.forEach((key, value) -> documentFields.put(key, documentFieldFromProto(value)));
        }

        metaFields = new HashMap<>();
        Map<String, DocumentFieldProto> metaFieldProtoMap = proto.getMetaFieldsMap();
        if (!metaFieldProtoMap.isEmpty()) {
            metaFieldProtoMap.forEach((key, value) -> metaFields.put(key, documentFieldFromProto(value)));
        }

        highlightFields = new HashMap<>();
        Map<String, HighlightFieldProto> highlightFieldProtoMap = proto.getHighlightFieldsMap();
        if (!highlightFieldProtoMap.isEmpty()) {
            highlightFieldProtoMap.forEach((key, value) -> highlightFields.put(key, highlightFieldFromProto(value)));
        }
    }

    public static NestedIdentityProto nestedIdentityToProto(SearchHit.NestedIdentity nestedIdentity) {
        NestedIdentityProto.Builder builder = NestedIdentityProto.newBuilder()
            .setField(nestedIdentity.getField().string())
            .setOffset(nestedIdentity.getOffset());

        if (nestedIdentity.getChild() != null) {
            builder.setChild(nestedIdentityToProto(nestedIdentity.getChild()));
        }

        return builder.build();
    }

    public static SearchHit.NestedIdentity nestedIdentityFromProto(NestedIdentityProto proto) {
        String field = proto.getField();
        int offset = proto.getOffset();

        SearchHit.NestedIdentity child = null;
        if (proto.hasChild()) {
            child = nestedIdentityFromProto(proto.getChild());
        }

        return new SearchHit.NestedIdentity(field, offset, child);
    }
}
