/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.serde;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.Explanation;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchSortValues;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.serde.proto.SearchHitsTransportProto.DocumentFieldProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.ExplanationProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.HighlightFieldProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.IndexProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchShardTargetProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchSortValuesProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.ShardIdProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * SerDe interfaces and protobuf SerDe implementations for some "primitive" types.
 * @opensearch.internal
 */
public class SerDe {

    public enum Strategy {
        PROTOBUF,
        NATIVE;
    }

    /**
     * Serialization/Deserialization exception.
     * @opensearch.internal
     */
    public static class SerializationException extends RuntimeException {
        public SerializationException(String message) {
            super(message);
        }

        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    interface nativeSerializer {
        void toNativeStream(StreamOutput out) throws IOException;

        void fromNativeStream(StreamInput in) throws IOException;
    }

    interface protobufSerializer {
        void toProtobufStream(StreamOutput out) throws IOException;

        void fromProtobufStream(StreamInput in) throws IOException;
    }

    // TODO: Lucene definitions should maybe be serialized as generic bytes arrays.
    static ExplanationProto explanationToProto(Explanation explanation) {
        ExplanationProto.Builder builder = ExplanationProto.newBuilder()
            .setMatch(explanation.isMatch())
            .setValue(explanation.getValue().longValue())
            .setDescription(explanation.getDescription());

        for (Explanation detail : explanation.getDetails()) {
            builder.addDetails(explanationToProto(detail));
        }

        return builder.build();
    }

    static Explanation explanationFromProto(ExplanationProto proto) {
        long value = proto.getValue();
        String description = proto.getDescription();
        Collection<Explanation> details = new ArrayList<>();

        for (ExplanationProto det : proto.getDetailsList()) {
            details.add(explanationFromProto(det));
        }

        if (proto.getMatch()) {
            return Explanation.match(value, description, details);
        }

        return Explanation.noMatch(description, details);
    }

    // Is there a reason to open a new stream for each object?
    // Seems simpler to write a single stream.
    static DocumentFieldProto documentFieldToProto(DocumentField field) {
        DocumentFieldProto.Builder builder = DocumentFieldProto.newBuilder().setName(field.getName());

        try (BytesStreamOutput docsOut = new BytesStreamOutput()) {
            docsOut.writeCollection(field.getValues(), StreamOutput::writeGenericValue);
            builder.addValues(ByteString.copyFrom(docsOut.bytes().toBytesRef().bytes));
        } catch (IOException e) {
            builder.addValues(ByteString.EMPTY);
        }

        return builder.build();
    }

    static DocumentField documentFieldFromProto(DocumentFieldProto proto) throws SerDe.SerializationException {
        String name = proto.getName();
        List<Object> values = new ArrayList<>(0);

        if (proto.getValuesCount() > 0) {
            BytesReference valuesBytes = new BytesArray(proto.getValues(0).toByteArray());
            try (StreamInput in = valuesBytes.streamInput()) {
                Object readValue = in.readGenericValue();
                values.add(readValue);
            } catch (IOException e) {
                throw new SerDe.SerializationException("Failed to deserialize DocumentField values from proto object", e);
            }
        }

        return new DocumentField(name, values);
    }

    static HighlightFieldProto highlightFieldToProto(HighlightField field) {
        HighlightFieldProto.Builder builder = HighlightFieldProto.newBuilder().setName(field.getName());

        for (Text frag : field.getFragments()) {
            builder.addFragments(frag.string());
        }

        return builder.build();
    }

    static HighlightField highlightFieldFromProto(HighlightFieldProto proto) {
        String name = proto.getName();
        Text[] fragments = new Text[proto.getFragmentsCount()];

        for (int i = 0; i < proto.getFragmentsCount(); i++) {
            fragments[i] = new Text(proto.getFragments(i));
        }

        return new HighlightField(name, fragments);
    }

    // See above comment for documentFieldToProto.
    static SearchSortValuesProto searchSortValuesToProto(SearchSortValues searchSortValues) {
        SearchSortValuesProto.Builder builder = SearchSortValuesProto.newBuilder();

        try (BytesStreamOutput formOut = new BytesStreamOutput()) {
            formOut.writeArray(Lucene::writeSortValue, searchSortValues.getFormattedSortValues());
            builder.addFormattedSortValues(ByteString.copyFrom(formOut.bytes().toBytesRef().bytes));
        } catch (IOException e) {
            builder.addFormattedSortValues(ByteString.EMPTY);
        }

        try (BytesStreamOutput rawOut = new BytesStreamOutput()) {
            rawOut.writeArray(Lucene::writeSortValue, searchSortValues.getFormattedSortValues());
            builder.addRawSortValues(ByteString.copyFrom(rawOut.bytes().toBytesRef().bytes));
        } catch (IOException e) {
            builder.addRawSortValues(ByteString.EMPTY);
        }

        return builder.build();
    }

    static SearchSortValues searchSortValuesFromProto(SearchSortValuesProto proto) throws SerDe.SerializationException {
        Object[] formattedSortValues = null;
        Object[] rawSortValues = null;

        if (proto.getFormattedSortValuesCount() > 0) {
            BytesReference formattedBytes = new BytesArray(proto.getFormattedSortValues(0).toByteArray());
            try (StreamInput formattedIn = formattedBytes.streamInput()) {
                formattedSortValues = formattedIn.readArray(Lucene::readSortValue, Object[]::new);
            } catch (IOException e) {
                throw new SerDe.SerializationException("Failed to deserialize SearchSortValues from proto object", e);
            }
        }

        if (proto.getRawSortValuesCount() > 0) {
            BytesReference rawBytes = new BytesArray(proto.getRawSortValues(0).toByteArray());
            try (StreamInput rawIn = rawBytes.streamInput()) {
                rawSortValues = rawIn.readArray(Lucene::readSortValue, Object[]::new);
            } catch (IOException e) {
                throw new SerDe.SerializationException("Failed to deserialize SearchSortValues from proto object", e);
            }
        }

        return new SearchSortValues(formattedSortValues, rawSortValues);
    }

    static SearchShardTargetProto searchShardTargetToProto(SearchShardTarget shardTarget) {
        return SearchShardTargetProto.newBuilder()
            .setNodeId(shardTarget.getNodeId())
            .setShardId(shardIdToProto(shardTarget.getShardId()))
            .setClusterAlias(shardTarget.getClusterAlias())
            .build();
    }

    public static SearchShardTarget searchShardTargetFromProto(SearchShardTargetProto proto) {
        String nodeId = proto.getNodeId();
        ShardId shardId = shardIdFromProto(proto.getShardId());
        String clusterAlias = proto.getClusterAlias();
        return new SearchShardTarget(nodeId, shardId, clusterAlias);
    }

    static ShardIdProto shardIdToProto(ShardId shardId) {
        return ShardIdProto.newBuilder()
            .setIndex(indexToProto(shardId.getIndex()))
            .setShardId(shardId.id())
            .setHashCode(shardId.hashCode())
            .build();
    }

    public static ShardId shardIdFromProto(ShardIdProto proto) {
        Index index = indexFromProto(proto.getIndex());
        int shardId = proto.getShardId();
        return new ShardId(index, shardId);
    }

    static IndexProto indexToProto(Index index) {
        return IndexProto.newBuilder().setName(index.getName()).setUuid(index.getUUID()).build();
    }

    public static Index indexFromProto(IndexProto proto) {
        String name = proto.getName();
        String uuid = proto.getUuid();
        return new Index(name, uuid);
    }
}
