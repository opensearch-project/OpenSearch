/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobuf;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.Explanation;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
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

import static org.opensearch.common.lucene.Lucene.readSortValue;
import static org.opensearch.common.lucene.Lucene.writeSortValue;

/**
 * SerDe interfaces and protobuf SerDe implementations for some "primitive" types.
 * @opensearch.internal
 */
public class ProtoSerDeHelpers {

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

    // TODO: Lucene definitions should maybe be serialized as generic bytes arrays.
    public static ExplanationProto explanationToProto(Explanation explanation) {
        ExplanationProto.Builder builder = ExplanationProto.newBuilder()
            .setMatch(explanation.isMatch())
            .setDescription(explanation.getDescription());

        Number num = explanation.getValue();
        if (num instanceof Long) {
            builder.setLongValue(num.longValue());
        } else if (num instanceof Integer) {
            builder.setIntValue(num.intValue());
        } else if (num instanceof Double) {
            builder.setDoubleValue(num.doubleValue());
        } else if (num instanceof Float) {
            builder.setFloatValue(num.floatValue());
        } else {
            throw new SerializationException("Unknown numeric type [" + num + "]");
        }

        for (Explanation detail : explanation.getDetails()) {
            builder.addDetails(explanationToProto(detail));
        }

        return builder.build();
    }

    public static Explanation explanationFromProto(ExplanationProto proto) {
        String description = proto.getDescription();
        Collection<Explanation> details = new ArrayList<>();

        Number val = null;
        switch (proto.getValueCase()) {
            case INT_VALUE:
                val = proto.getIntValue();
                break;
            case LONG_VALUE:
                val = proto.getLongValue();
                break;
            case FLOAT_VALUE:
                val = proto.getFloatValue();
                break;
            case DOUBLE_VALUE:
                val = proto.getDoubleValue();
                break;
            default:
                // No value, leave null
        }

        for (ExplanationProto det : proto.getDetailsList()) {
            details.add(explanationFromProto(det));
        }

        if (proto.getMatch()) {
            return Explanation.match(val, description, details);
        }

        return Explanation.noMatch(description, details);
    }

    // TODO: Can we write all objects to a single stream?
    public static DocumentFieldProto documentFieldToProto(DocumentField field) {
        DocumentFieldProto.Builder builder = DocumentFieldProto.newBuilder().setName(field.getName());

        for (Object value : field.getValues()) {
            try (BytesStreamOutput docsOut = new BytesStreamOutput()) {
                docsOut.writeGenericValue(value);
                builder.addValues(ByteString.copyFrom(docsOut.bytes().toBytesRef().bytes));
            } catch (IOException e) {
                builder.addValues(ByteString.EMPTY);
            }
        }

        return builder.build();
    }

    public static DocumentField documentFieldFromProto(DocumentFieldProto proto) throws ProtoSerDeHelpers.SerializationException {
        String name = proto.getName();
        ArrayList<Object> values = new ArrayList<>();

        for (int i = 0; i < proto.getValuesCount(); i++) {
            BytesReference valuesBytes = new BytesArray(proto.getValues(i).toByteArray());
            try (StreamInput in = valuesBytes.streamInput()) {
                values.add(in.readGenericValue());
            } catch (IOException e) {
                throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize DocumentField values from proto object", e);
            }
        }

        return new DocumentField(name, values);
    }

    public static HighlightFieldProto highlightFieldToProto(HighlightField field) {
        HighlightFieldProto.Builder builder = HighlightFieldProto.newBuilder().setName(field.getName()).setFragsNull(true);

        if (field.getFragments() != null) {
            builder.setFragsNull(false);
            for (Text frag : field.getFragments()) {
                builder.addFragments(frag.string());
            }
        }

        return builder.build();
    }

    public static HighlightField highlightFieldFromProto(HighlightFieldProto proto) {
        String name = proto.getName();
        Text[] fragments = null;

        if (!proto.getFragsNull()) {
            fragments = new Text[proto.getFragmentsCount()];
            for (int i = 0; i < proto.getFragmentsCount(); i++) {
                fragments[i] = new Text(proto.getFragments(i));
            }
        }

        return new HighlightField(name, fragments);
    }

    // TODO: Can we write all objects to a single stream?
    public static SearchSortValuesProto searchSortValuesToProto(SearchSortValues searchSortValues) {
        SearchSortValuesProto.Builder builder = SearchSortValuesProto.newBuilder();

        for (Object value : searchSortValues.getFormattedSortValues()) {
            try (BytesStreamOutput docsOut = new BytesStreamOutput()) {
                writeSortValue(docsOut, value);
                builder.addFormattedSortValues(ByteString.copyFrom(docsOut.bytes().toBytesRef().bytes));
            } catch (IOException e) {
                builder.addFormattedSortValues(ByteString.EMPTY);
            }
        }

        for (Object value : searchSortValues.getRawSortValues()) {
            try (BytesStreamOutput docsOut = new BytesStreamOutput()) {
                writeSortValue(docsOut, value);
                builder.addRawSortValues(ByteString.copyFrom(docsOut.bytes().toBytesRef().bytes));
            } catch (IOException e) {
                builder.addRawSortValues(ByteString.EMPTY);
            }
        }

        return builder.build();
    }

    public static SearchSortValues searchSortValuesFromProto(SearchSortValuesProto proto) throws ProtoSerDeHelpers.SerializationException {
        Object[] formattedSortValues = new Object[proto.getFormattedSortValuesCount()];
        Object[] rawSortValues = new Object[proto.getRawSortValuesCount()];

        for (int i = 0; i < formattedSortValues.length; i++) {
            BytesReference valuesBytes = new BytesArray(proto.getFormattedSortValues(i).toByteArray());
            try (StreamInput in = valuesBytes.streamInput()) {
                formattedSortValues[i] = readSortValue(in);
            } catch (IOException e) {
                throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize SearchSortValues from protobuf", e);
            }
        }

        for (int i = 0; i < rawSortValues.length; i++) {
            BytesReference valuesBytes = new BytesArray(proto.getRawSortValues(i).toByteArray());
            try (StreamInput in = valuesBytes.streamInput()) {
                rawSortValues[i] = readSortValue(in);
            } catch (IOException e) {
                throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize SearchSortValues from protobuf", e);
            }
        }

        return new SearchSortValues(formattedSortValues, rawSortValues);
    }

    public static SearchShardTargetProto searchShardTargetToProto(SearchShardTarget shardTarget) {
        SearchShardTargetProto.Builder builder = SearchShardTargetProto.newBuilder()
            .setNodeId(shardTarget.getNodeId())
            .setShardId(shardIdToProto(shardTarget.getShardId()));

        if (shardTarget.getClusterAlias() != null) {
            builder.setClusterAlias(shardTarget.getClusterAlias());
        }

        return builder.build();
    }

    public static SearchShardTarget searchShardTargetFromProto(SearchShardTargetProto proto) {
        String nodeId = proto.getNodeId();
        ShardId shardId = shardIdFromProto(proto.getShardId());

        String clusterAlias = null;
        if (proto.hasClusterAlias()) {
            clusterAlias = proto.getClusterAlias();
        }

        return new SearchShardTarget(nodeId, shardId, clusterAlias);
    }

    public static ShardIdProto shardIdToProto(ShardId shardId) {
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

    public static IndexProto indexToProto(Index index) {
        return IndexProto.newBuilder().setName(index.getName()).setUuid(index.getUUID()).build();
    }

    public static Index indexFromProto(IndexProto proto) {
        String name = proto.getName();
        String uuid = proto.getUuid();
        return new Index(name, uuid);
    }
}
