/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.common.annotation.ExperimentalApi;
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
import java.util.Objects;

/**
 * A single add- or remove-backing-index operation within a {@link ModifyDataStreamsAction} request.
 *
 * @opensearch.api
 */
@ExperimentalApi
public record DataStreamAction(Type type, String dataStream, String index) implements Writeable, ToXContentObject {

    /**
     * The type of modification applied to a data stream.
     *
     * @opensearch.api
     */
    @ExperimentalApi
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

    public DataStreamAction {
        Objects.requireNonNull(type, "[type] is required");
        Objects.requireNonNull(dataStream, "[data_stream] is required");
        Objects.requireNonNull(index, "[index] is required");
    }

    public static DataStreamAction addBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.ADD_BACKING_INDEX, dataStream, index);
    }

    public static DataStreamAction removeBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.REMOVE_BACKING_INDEX, dataStream, index);
    }

    public DataStreamAction(StreamInput in) throws IOException {
        this(Type.fromValue(in.readByte()), in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type.value());
        out.writeString(dataStream);
        out.writeString(index);
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
            args -> new DataStreamAction(type, (String) args[0], (String) args[1])
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
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private static class ActionHolder {
        private DataStreamAction action;
    }
}
