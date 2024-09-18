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
import org.apache.lucene.search.SortField;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.proto.search.SearchHitsProtoDef.DocumentFieldProto;
import org.opensearch.proto.search.SearchHitsProtoDef.ExplanationProto;
import org.opensearch.proto.search.SearchHitsProtoDef.HighlightFieldProto;
import org.opensearch.proto.search.SearchHitsProtoDef.IndexProto;
import org.opensearch.proto.search.SearchHitsProtoDef.SearchShardTargetProto;
import org.opensearch.proto.search.SearchHitsProtoDef.ShardIdProto;
import org.opensearch.proto.search.SearchHitsProtoDef.SortFieldProto;
import org.opensearch.proto.search.SearchHitsProtoDef.SortTypeProto;
import org.opensearch.proto.search.SearchHitsProtoDef.GenericObjectProto;
import org.opensearch.proto.search.SearchHitsProtoDef.MissingValueProto;
import org.opensearch.transport.TransportSerializationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Serialization helpers for common objects shared across multiple protobuf types.
 * @opensearch.internal
 */
public class ProtoSerDeHelpers {
    public static GenericObjectProto genericObjectToProto(Object obj) {
        GenericObjectProto.Builder builder = GenericObjectProto.newBuilder();

        try (BytesStreamOutput docsOut = new BytesStreamOutput()) {
            docsOut.writeGenericValue(obj);
            builder.setValue(ByteString.copyFrom(docsOut.bytes().toBytesRef().bytes));
        } catch (IOException e) {
            builder.setValue(ByteString.EMPTY);
        }

        return builder.build();
    }

    public static Object genericObjectFromProto(GenericObjectProto proto) {
        Object obj;
        BytesReference valuesBytes = new BytesArray(proto.getValue().toByteArray());

        try (StreamInput in = valuesBytes.streamInput()) {
            obj = in.readGenericValue();
        } catch (IOException e) {
            throw new TransportSerializationException("Failed to deserialize DocumentField values from proto object", e);
        }

        return obj;
    }

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
            throw new TransportSerializationException("Unknown numeric type [" + num + "]");
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
            assert val != null;
            return Explanation.match(val, description, details);
        }

        return Explanation.noMatch(description, details);
    }

    public static DocumentFieldProto documentFieldToProto(DocumentField field) {
        DocumentFieldProto.Builder builder = DocumentFieldProto.newBuilder().setName(field.getName());

        for (Object value : field.getValues()) {
            builder.addValues(genericObjectToProto(value));
        }

        return builder.build();
    }

    public static DocumentField documentFieldFromProto(DocumentFieldProto proto) throws TransportSerializationException {
        String name = proto.getName();
        ArrayList<Object> values = new ArrayList<>();

        for (int i = 0; i < proto.getValuesCount(); i++) {
            GenericObjectProto v = proto.getValues(i);
            values.add(genericObjectFromProto(v));
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

    public static SortFieldProto sortFieldToProto(SortField sortField) {
        SortFieldProto.Builder builder = SortFieldProto.newBuilder()
            .setType(sortTypeToProto(sortField.getType()))
            .setReverse(sortField.getReverse());

        if (sortField.getMissingValue() != null) {
            builder.setMissingValue(missingValueToProto(sortField.getMissingValue()));
        }

        if (sortField.getField() != null) {
            builder.setField(sortField.getField());
        }

        return builder.build();
    }

    public static SortField sortFieldFromProto(SortFieldProto proto) {
        String field = null;
        if (proto.hasField()) {
            field = proto.getField();
        }

        SortField sortField = new SortField(
            field,
            sortTypeFromProto(proto.getType()),
            proto.getReverse());

        if (proto.hasMissingValue()) {
            sortField.setMissingValue(missingValueFromProto(proto.getMissingValue()));
        }

        return sortField;
    }

    public static SortTypeProto sortTypeToProto(SortField.Type sortType) {
        return SortTypeProto.forNumber(sortType.ordinal());
    }

    public static SortField.Type sortTypeFromProto(SortTypeProto proto) {
        return SortField.Type.values()[proto.getNumber()];
    }

    public static MissingValueProto missingValueToProto(Object missingValue) {
        MissingValueProto.Builder builder = MissingValueProto.newBuilder();

        if (missingValue == SortField.STRING_FIRST) {
            builder.setIntVal(1);
        } else if (missingValue == SortField.STRING_LAST) {
            builder.setIntVal(2);
        } else {
            builder.setObjVal(genericObjectToProto(missingValue));
        }

        return builder.build();
    }

    public static Object missingValueFromProto(MissingValueProto proto) {
        switch (proto.getValueCase()) {
            case INT_VAL:
                if (proto.getIntVal() == 1) { return SortField.STRING_FIRST; }
                if (proto.getIntVal() == 2) { return SortField.STRING_LAST; }
            case OBJ_VAL:
                return genericObjectFromProto(proto.getObjVal());
            default:
                throw new TransportSerializationException("Unexpected value case: " + proto.getValueCase());
        }
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
