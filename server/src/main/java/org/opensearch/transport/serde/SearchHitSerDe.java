/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.serde;

import com.google.protobuf.ByteString;
import org.opensearch.Version;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.text.Text;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchSortValues;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.serde.proto.SearchHitsTransportProto.NestedIdentityProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchHitProto;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.lucene.Lucene.readExplanation;
import static org.opensearch.transport.serde.SerDe.documentFieldFromProto;
import static org.opensearch.transport.serde.SerDe.documentFieldToProto;
import static org.opensearch.transport.serde.SerDe.explanationFromProto;
import static org.opensearch.transport.serde.SerDe.explanationToProto;
import static org.opensearch.transport.serde.SerDe.highlightFieldFromProto;
import static org.opensearch.transport.serde.SerDe.highlightFieldToProto;
import static org.opensearch.transport.serde.SerDe.searchShardTargetFromProto;
import static org.opensearch.transport.serde.SerDe.searchShardTargetToProto;
import static org.opensearch.transport.serde.SerDe.searchSortValuesFromProto;
import static org.opensearch.transport.serde.SerDe.searchSortValuesToProto;

/**
 * Serialization/Deserialization implementations for SearchHit.
 * @opensearch.internal
 */
public class SearchHitSerDe extends SearchHit implements SerDe.nativeSerializer, SerDe.protobufSerializer {
    SerDe.Strategy strategy = SerDe.Strategy.NATIVE;

    public SearchHitSerDe(SearchHit hit, SerDe.Strategy strategy) {
        super(hit);
        this.strategy = strategy;
    }

    public SearchHitSerDe(SerDe.Strategy strategy, StreamInput in) throws IOException {
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

    public SearchHitSerDe(StreamInput in) throws IOException {
        fromNativeStream(in);
    }

    public SearchHitSerDe(SearchHitProto proto) throws IOException {
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
        SearchHitProto proto = SearchHitProto.parseFrom(in);
        fromProto(proto);
    }

    @Override
    public void toNativeStream(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public void fromNativeStream(StreamInput in) throws IOException {
        docId = -1;
        score = in.readFloat();
        id = in.readOptionalText();
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readOptionalText();
        }

        nestedIdentity = in.readOptionalWriteable(NestedIdentity::new);
        version = in.readLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        source = in.readBytesReference();
        if (source.length() == 0) {
            source = null;
        }
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        documentFields = in.readMap(StreamInput::readString, DocumentField::new);
        metaFields = in.readMap(StreamInput::readString, DocumentField::new);

        int size = in.readVInt();
        if (size == 0) {
            highlightFields = emptyMap();
        } else if (size == 1) {
            HighlightField field = new HighlightField(in);
            highlightFields = singletonMap(field.name(), field);
        } else {
            Map<String, HighlightField> hlFields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                HighlightField field = new HighlightField(in);
                hlFields.put(field.name(), field);
            }
            highlightFields = unmodifiableMap(hlFields);
        }

        sortValues = new SearchSortValues(in);

        size = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_2_13_0)) {
            if (size > 0) {
                Map<String, Float> tempMap = in.readMap(StreamInput::readString, StreamInput::readFloat);
                matchedQueries = tempMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new)
                    );
            }
        } else {
            matchedQueries = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                matchedQueries.put(in.readString(), Float.NaN);
            }
        }
        shard = in.readOptionalWriteable(SearchShardTarget::new);
        if (shard != null) {
            index = shard.getIndex();
            clusterAlias = shard.getClusterAlias();
        }

        size = in.readVInt();
        if (size > 0) {
            innerHits = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                SearchHits value = new SearchHitsSerDe(strategy, in);
                innerHits.put(key, value);
            }
        } else {
            innerHits = null;
        }
    }

    SearchHitProto toProto() {
        SearchHitProto.Builder builder = SearchHitProto.newBuilder()
            .setScore(score)
            .setId(id.string())
            .setVersion(version)
            .setSeqNo(seqNo)
            .setPrimaryTerm(primaryTerm);

        builder.setNestedIdentity(nestedIdentityToProto(nestedIdentity));
        builder.setSource(ByteString.copyFrom(source.toBytesRef().bytes));
        builder.setExplanation(explanationToProto(explanation));
        builder.setSortValues(searchSortValuesToProto(sortValues));

        documentFields.forEach((key, value) -> builder.putDocumentFields(key, documentFieldToProto(value)));

        metaFields.forEach((key, value) -> builder.putMetaFields(key, documentFieldToProto(value)));

        highlightFields.forEach((key, value) -> builder.putHighlightFields(key, highlightFieldToProto(value)));

        matchedQueries.forEach(builder::putMatchedQueries);

        // shard is optional
        if (shard != null) {
            builder.setShard(searchShardTargetToProto(shard));
        }

        innerHits.forEach((key, value) -> builder.putInnerHits(key, new SearchHitsSerDe(value, strategy).toProto()));

        return builder.build();
    }

    void fromProto(SearchHitProto proto) throws SerDe.SerializationException {
        docId = -1;
        score = proto.getScore();
        seqNo = proto.getSeqNo();
        version = proto.getVersion();
        primaryTerm = proto.getPrimaryTerm();
        id = new Text(proto.getId());
        source = BytesReference.fromByteBuffer(proto.getSource().asReadOnlyByteBuffer());
        explanation = explanationFromProto(proto.getExplanation());
        sortValues = searchSortValuesFromProto(proto.getSortValues());
        nestedIdentity = nestedIdentityFromProto(proto.getNestedIdentity());
        matchedQueries = proto.getMatchedQueriesMap();

        documentFields = new HashMap<>();
        proto.getDocumentFieldsMap().forEach((key, value) -> documentFields.put(key, documentFieldFromProto(value)));

        metaFields = new HashMap<>();
        proto.getMetaFieldsMap().forEach((key, value) -> metaFields.put(key, documentFieldFromProto(value)));

        highlightFields = new HashMap<>();
        proto.getHighlightFieldsMap().forEach((key, value) -> highlightFields.put(key, highlightFieldFromProto(value)));

        innerHits = new HashMap<>();
        proto.getInnerHitsMap().forEach((key, value) -> innerHits.put(key, new SearchHitsSerDe(value)));

        shard = searchShardTargetFromProto(proto.getShard());
        index = shard.getIndex();
        clusterAlias = shard.getClusterAlias();
    }

    static NestedIdentityProto nestedIdentityToProto(SearchHit.NestedIdentity nestedIdentity) {
        NestedIdentityProto.Builder builder = NestedIdentityProto.newBuilder()
            .setField(nestedIdentity.getField().string())
            .setOffset(nestedIdentity.getOffset());

        if (nestedIdentity.getChild() != null) {
            builder.setChild(nestedIdentityToProto(nestedIdentity.getChild()));
        }

        return builder.build();
    }

    static SearchHit.NestedIdentity nestedIdentityFromProto(NestedIdentityProto proto) {
        String field = proto.getField();
        int offset = proto.getOffset();

        SearchHit.NestedIdentity child = null;
        if (proto.hasChild()) {
            child = nestedIdentityFromProto(proto.getChild());
        }

        return new SearchHit.NestedIdentity(field, offset, child);
    }
}
