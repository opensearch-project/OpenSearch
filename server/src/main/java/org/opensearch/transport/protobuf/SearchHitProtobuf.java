/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobuf;

import com.google.protobuf.ByteString;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.text.Text;
import org.opensearch.proto.search.SearchHitsProtoDef;
import org.opensearch.search.SearchHit;
import org.opensearch.proto.search.SearchHitsProtoDef.SearchHitProto;
import org.opensearch.proto.search.SearchHitsProtoDef.NestedIdentityProto;
import org.opensearch.search.SearchSortValues;
import org.opensearch.transport.TransportSerializationException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;

import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetToProto;

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

        // innerHits is nullable
        if (proto.getInnerHitsCount() < 1) {
            innerHits = null;
        } else {
            innerHits = new HashMap<>();
            proto.getInnerHitsMap().forEach((key, value) -> innerHits.put(key, new SearchHitsProtobuf(value)));
        }

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

    public static SearchHitsProtoDef.SearchSortValuesProto searchSortValuesToProto(SearchSortValues searchSortValues) {
        SearchHitsProtoDef.SearchSortValuesProto.Builder builder = SearchHitsProtoDef.SearchSortValuesProto.newBuilder();

        for (Object value : searchSortValues.getFormattedSortValues()) {
            builder.addFormattedSortValues(sortValueToProto(value));
        }

        for (Object value : searchSortValues.getRawSortValues()) {
            builder.addRawSortValues(sortValueToProto(value));
        }

        return builder.build();
    }

    public static SearchSortValues searchSortValuesFromProto(SearchHitsProtoDef.SearchSortValuesProto proto) throws TransportSerializationException {
        Object[] formattedSortValues = new Object[proto.getFormattedSortValuesCount()];
        Object[] rawSortValues = new Object[proto.getRawSortValuesCount()];

        for (int i = 0; i < formattedSortValues.length; i++) {
            SearchHitsProtoDef.SortValueProto sortProto = proto.getFormattedSortValues(i);
            formattedSortValues[i] = sortValueFromProto(sortProto);
        }

        for (int i = 0; i < rawSortValues.length; i++) {
            SearchHitsProtoDef.SortValueProto sortProto = proto.getRawSortValues(i);
            rawSortValues[i] = sortValueFromProto(sortProto);
        }

        return new SearchSortValues(formattedSortValues, rawSortValues);
    }

    public static SearchHitsProtoDef.SortValueProto sortValueToProto(Object sortValue) throws TransportSerializationException {
        SearchHitsProtoDef.SortValueProto.Builder builder = SearchHitsProtoDef.SortValueProto.newBuilder();

        if (sortValue == null) {
            builder.setIsNull(true);
        } else if (sortValue.getClass().equals(String.class)) {
            builder.setStringValue((String) sortValue);
        } else if (sortValue.getClass().equals(Integer.class)) {
            builder.setIntValue((Integer) sortValue);
        } else if (sortValue.getClass().equals(Long.class)) {
            builder.setLongValue((Long) sortValue);
        } else if (sortValue.getClass().equals(Float.class)) {
            builder.setFloatValue((Float) sortValue);
        } else if (sortValue.getClass().equals(Double.class)) {
            builder.setDoubleValue((Double) sortValue);
        } else if (sortValue.getClass().equals(Byte.class)) {
            builder.setByteValue((Byte) sortValue);
        } else if (sortValue.getClass().equals(Short.class)) {
            builder.setShortValue((Short) sortValue);
        } else if (sortValue.getClass().equals(Boolean.class)) {
            builder.setBoolValue((Boolean) sortValue);
        } else if (sortValue.getClass().equals(BytesRef.class)) {
            builder.setBytesValue(ByteString.copyFrom(
                ((BytesRef) sortValue).bytes,
                ((BytesRef) sortValue).offset,
                ((BytesRef) sortValue).length));
        } else if (sortValue.getClass().equals(BigInteger.class)) {
            builder.setBigIntegerValue(sortValue.toString());
        } else {
            throw new TransportSerializationException("Unexpected sortValue: " + sortValue);
        }

        return builder.build();
    }

    public static Object sortValueFromProto(SearchHitsProtoDef.SortValueProto proto) throws TransportSerializationException {
        switch (proto.getValueCase()) {
            case STRING_VALUE:
                return proto.getStringValue();
            case INT_VALUE:
                return proto.getIntValue();
            case LONG_VALUE:
                return proto.getLongValue();
            case FLOAT_VALUE:
                return proto.getFloatValue();
            case DOUBLE_VALUE:
                return proto.getDoubleValue();
            case BYTE_VALUE:
                return (byte) proto.getByteValue();
            case SHORT_VALUE:
                return (short) proto.getShortValue();
            case BOOL_VALUE:
                return proto.getBoolValue();
            case BYTES_VALUE:
                ByteString byteString = proto.getBytesValue();
                return new BytesRef(byteString.toByteArray());
            case BIG_INTEGER_VALUE:
                return new BigInteger(proto.getBigIntegerValue());
            case IS_NULL:
                return null;
        }

        throw new TransportSerializationException("Unexpected value case: " + proto.getValueCase());
    }
}
