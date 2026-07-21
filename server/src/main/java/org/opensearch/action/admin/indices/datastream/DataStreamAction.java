/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a single operation applied to a data stream's backing-index set as part of a
 * {@link ModifyDataStreamsAction} request. These are metadata-only mutations; they never create, delete, restore, or
 * move shards. The data stream's generation is derived automatically from its backing indices, so it is not part of the
 * action surface.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class DataStreamAction implements Writeable, ToXContentObject {

    /**
     * The type of modification applied to a data stream.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.8.0")
    public enum Type {
        ADD_BACKING_INDEX((byte) 0, "add_backing_index"),
        REMOVE_BACKING_INDEX((byte) 1, "remove_backing_index");

        private final byte value;
        private final String fieldName;

        Type(byte value, String fieldName) {
            this.value = value;
            this.fieldName = fieldName;
        }

        public byte value() {
            return value;
        }

        public String fieldName() {
            return fieldName;
        }

        public static Type fromValue(byte value) {
            switch (value) {
                case 0:
                    return ADD_BACKING_INDEX;
                case 1:
                    return REMOVE_BACKING_INDEX;
                default:
                    throw new IllegalArgumentException("no data stream action type for [" + value + "]");
            }
        }
    }

    private final Type type;
    private final String dataStream;
    private String index;

    public static DataStreamAction addBackingIndex(String dataStream, String index) {
        DataStreamAction action = new DataStreamAction(Type.ADD_BACKING_INDEX, dataStream);
        action.index = Objects.requireNonNull(index, "[index] is required for [add_backing_index]");
        return action;
    }

    public static DataStreamAction removeBackingIndex(String dataStream, String index) {
        DataStreamAction action = new DataStreamAction(Type.REMOVE_BACKING_INDEX, dataStream);
        action.index = Objects.requireNonNull(index, "[index] is required for [remove_backing_index]");
        return action;
    }

    private DataStreamAction(Type type, String dataStream) {
        this.type = Objects.requireNonNull(type, "[type] is required");
        this.dataStream = Objects.requireNonNull(dataStream, "[data_stream] is required");
    }

    public DataStreamAction(StreamInput in) throws IOException {
        this.type = Type.fromValue(in.readByte());
        this.dataStream = in.readString();
        this.index = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type.value());
        out.writeString(dataStream);
        out.writeOptionalString(index);
    }

    public Type getType() {
        return type;
    }

    public String getDataStream() {
        return dataStream;
    }

    public String getIndex() {
        return index;
    }

    private static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");
    private static final ParseField INDEX_FIELD = new ParseField("index");

    // Parser per action type; the enclosing action is a single-key object whose key is the action type.
    private static final ConstructingObjectParser<DataStreamAction, Void> ADD_PARSER = actionParser(Type.ADD_BACKING_INDEX);
    private static final ConstructingObjectParser<DataStreamAction, Void> REMOVE_PARSER = actionParser(Type.REMOVE_BACKING_INDEX);

    private static final ObjectParser<ActionHolder, Void> PARSER = new ObjectParser<>("data_stream_action", ActionHolder::new);

    static {
        PARSER.declareObject((holder, action) -> holder.action = action, ADD_PARSER, new ParseField(Type.ADD_BACKING_INDEX.fieldName()));
        PARSER.declareObject(
            (holder, action) -> holder.action = action,
            REMOVE_PARSER,
            new ParseField(Type.REMOVE_BACKING_INDEX.fieldName())
        );
    }

    private static ConstructingObjectParser<DataStreamAction, Void> actionParser(Type type) {
        ConstructingObjectParser<DataStreamAction, Void> parser = new ConstructingObjectParser<>(
            type.fieldName(),
            false,
            (args, unused) -> {
                DataStreamAction action = new DataStreamAction(type, (String) args[0]);
                action.index = (String) args[1];
                return action;
            }
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), DATA_STREAM_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        return parser;
    }

    public static DataStreamAction fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).action;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(type.fieldName());
        builder.field(DATA_STREAM_FIELD.getPreferredName(), dataStream);
        if (index != null) {
            builder.field(INDEX_FIELD.getPreferredName(), index);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStreamAction that = (DataStreamAction) o;
        return type == that.type && Objects.equals(dataStream, that.dataStream) && Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, dataStream, index);
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "DataStreamAction{type=%s, dataStream=%s, index=%s}", type, dataStream, index);
    }

    private static class ActionHolder {
        private DataStreamAction action;
    }
}
