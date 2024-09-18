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
import org.opensearch.proto.search.SearchHitsProtoDef.SearchHitProto;
import org.opensearch.proto.search.SearchHitsProtoDef.NestedIdentityProto;

import java.io.IOException;
import java.util.HashMap;

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
            .setPrimaryTerm(primaryTerm)
            .setSortValues(searchSortValuesToProto(sortValues));

        documentFields.forEach((key, value) -> builder.putDocumentFields(key, documentFieldToProto(value)));
        metaFields.forEach((key, value) -> builder.putMetaFields(key, documentFieldToProto(value)));
        matchedQueries.forEach(builder::putMatchedQueries);

        if (highlightFields != null) { highlightFields.forEach((key, value) -> builder.putHighlightFields(key, highlightFieldToProto(value))); }
        if (innerHits != null) { innerHits.forEach((key, value) -> builder.putInnerHits(key, new SearchHitsProtobuf(value).toProto())); }

        if (source != null) { builder.setSource(ByteString.copyFrom(source.toBytesRef().bytes)); }
        if (id != null) { builder.setId(id.string()); }
        if (nestedIdentity != null) { builder.setNestedIdentity(nestedIdentityToProto(nestedIdentity)); }
        if (shard != null) { builder.setShard(searchShardTargetToProto(shard)); }
        if (explanation != null) { builder.setExplanation(explanationToProto(explanation)); }

        return builder.build();
    }

    void fromProto(SearchHitProto proto) {
        docId = -1;
        score = proto.getScore();
        version = proto.getVersion();
        seqNo = proto.getSeqNo();
        primaryTerm = proto.getPrimaryTerm();
        sortValues = searchSortValuesFromProto(proto.getSortValues());

        documentFields = new HashMap<>();
        proto.getDocumentFieldsMap().forEach((key, value) -> documentFields.put(key, documentFieldFromProto(value)));

        metaFields = new HashMap<>();
        proto.getMetaFieldsMap().forEach((key, value) -> metaFields.put(key, documentFieldFromProto(value)));

        matchedQueries = proto.getMatchedQueriesMap();

        highlightFields = new HashMap<>();
        proto.getHighlightFieldsMap().forEach((key, value) -> highlightFields.put(key, highlightFieldFromProto(value)));

        innerHits = new HashMap<>();
        proto.getInnerHitsMap().forEach((key, value) -> innerHits.put(key, new SearchHitsProtobuf(value)));

        source = proto.hasSource()? BytesReference.fromByteBuffer(proto.getSource().asReadOnlyByteBuffer()) : null;
        id = proto.hasId()? new Text(proto.getId()) : null;
        nestedIdentity = proto.hasNestedIdentity()? nestedIdentityFromProto(proto.getNestedIdentity()) : null;
        explanation = proto.hasExplanation()? explanationFromProto(proto.getExplanation()) : null;

        if (proto.hasShard()) {
            shard = searchShardTargetFromProto(proto.getShard());
            index = shard.getIndex();
            clusterAlias = shard.getClusterAlias();
        } else {
            shard = null;
            index = null;
            clusterAlias = null;
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
