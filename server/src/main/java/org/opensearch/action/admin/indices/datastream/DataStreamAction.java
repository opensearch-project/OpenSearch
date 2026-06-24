/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Operations on a data stream's backing indices that are submitted as part of a
 * {@link ModifyDataStreamsAction} request. A single {@code DataStreamAction} either adds or removes a single
 * backing index from a single data stream.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class DataStreamAction implements Writeable, ToXContentObject {

    /**
     * The type of action to perform on a data stream's backing indices.
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
    private String dataStream;
    private String index;

    public static DataStreamAction addBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.ADD_BACKING_INDEX, dataStream, index);
    }

    public static DataStreamAction removeBackingIndex(String dataStream, String index) {
        return new DataStreamAction(Type.REMOVE_BACKING_INDEX, dataStream, index);
    }

    private DataStreamAction(Type type) {
        this.type = type;
    }

    private DataStreamAction(Type type, String dataStream, String index) {
        this.type = type;
        this.dataStream = dataStream;
        this.index = index;
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

    public DataStreamAction(StreamInput in) throws IOException {
        this.type = Type.fromValue(in.readByte());
        this.dataStream = in.readString();
        this.index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type.value());
        out.writeString(dataStream);
        out.writeString(index);
    }

    private static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");
    private static final ParseField INDEX_FIELD = new ParseField("index");

    private static final ObjectParser<DataStreamAction, Void> ADD_BACKING_INDEX_PARSER = parser(
        Type.ADD_BACKING_INDEX.fieldName(),
        () -> new DataStreamAction(Type.ADD_BACKING_INDEX)
    );
    private static final ObjectParser<DataStreamAction, Void> REMOVE_BACKING_INDEX_PARSER = parser(
        Type.REMOVE_BACKING_INDEX.fieldName(),
        () -> new DataStreamAction(Type.REMOVE_BACKING_INDEX)
    );

    private static ObjectParser<DataStreamAction, Void> parser(String name, Supplier<DataStreamAction> supplier) {
        ObjectParser<DataStreamAction, Void> parser = new ObjectParser<>(name, supplier);
        parser.declareString((action, dataStream) -> action.dataStream = dataStream, DATA_STREAM_FIELD);
        parser.declareString((action, index) -> action.index = index, INDEX_FIELD);
        return parser;
    }

    public static final ConstructingObjectParser<DataStreamAction, Void> PARSER = new ConstructingObjectParser<>(
        "data_stream_action",
        a -> {
            // only a single action is allowed per entry
            DataStreamAction action = null;
            int actionCount = 0;
            for (Object o : a) {
                if (o != null) {
                    action = (DataStreamAction) o;
                    actionCount++;
                }
            }
            if (actionCount == 0) {
                throw new IllegalArgumentException("no data stream operation declared on operation entry");
            }
            if (actionCount > 1) {
                throw new IllegalArgumentException("too many data stream operations declared on operation entry");
            }
            return action;
        }
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), ADD_BACKING_INDEX_PARSER, new ParseField(Type.ADD_BACKING_INDEX.fieldName()));
        PARSER.declareObject(optionalConstructorArg(), REMOVE_BACKING_INDEX_PARSER, new ParseField(Type.REMOVE_BACKING_INDEX.fieldName()));
    }

    public static DataStreamAction fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
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
}
