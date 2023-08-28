/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.search;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class GenericSearchExtBuilder extends SearchExtBuilder {

    public final static ParseField EXT_BUILDER_NAME = new ParseField("generic_ext");

    private final Object simpleVal;
    private final Map<String, Object> mapVal;
    private final List<Object> arrayVal;
    private final ValueType valueType;

    private enum ValueType {
        SIMPLE(0),
        MAP(1),
        LIST(2);

        private final int value;

        ValueType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        static ValueType fromInt(int value) {
            switch (value) {
                case 0:
                    return SIMPLE;
                case 1:
                    return MAP;
                case 2:
                    return LIST;
                default:
                    throw new IllegalArgumentException("Unsupported value: " + value);
            }
        }
    }

    public GenericSearchExtBuilder(Object simpleVal) {
        this.simpleVal = simpleVal;
        this.mapVal = null;
        this.arrayVal = null;
        this.valueType = ValueType.SIMPLE;
    }

    public GenericSearchExtBuilder(Map<String, Object> mapVal) {
        this.simpleVal = null;
        this.mapVal = mapVal;
        this.arrayVal = null;
        this.valueType = ValueType.MAP;
    }

    public GenericSearchExtBuilder(List<Object> arrayVal) {
        this.simpleVal = null;
        this.mapVal = null;
        this.arrayVal = arrayVal;
        this.valueType = ValueType.LIST;
    }

    public GenericSearchExtBuilder(StreamInput in) throws IOException {
        valueType = ValueType.fromInt(in.readInt());
        switch (valueType) {
            case SIMPLE:
                simpleVal = in.readGenericValue();
                mapVal = null;
                arrayVal = null;
                break;
            case MAP:
                mapVal = in.readMap();
                simpleVal = null;
                arrayVal = null;
                break;
            case LIST:
                simpleVal = null;
                mapVal = null;
                int size = in.readVInt();
                arrayVal = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    arrayVal.add(in.readGenericValue());
                }
                break;
            default:
                throw new IllegalStateException("Unable to construct GenericSearchExtBuilder from incoming stream.");
        }
    }

    public static GenericSearchExtBuilder fromXContent(XContentParser parser) throws IOException {
        // Look at the parser's next token.
        // If it's START_OBJECT, parse as map, if it's START_ARRAY, parse as list, else
        // parse as simpleVal
        XContentParser.Token token = parser.currentToken();
        GenericSearchExtBuilder extBuilder;
        if (token == XContentParser.Token.START_OBJECT) {
            extBuilder = new GenericSearchExtBuilder(parser.map());
        } else if (token == XContentParser.Token.START_ARRAY) {
            extBuilder = new GenericSearchExtBuilder(parser.list());
        } else if (token.isValue()) {
            extBuilder = new GenericSearchExtBuilder(parser.objectText()); // .objectBytes() ??
        } else {
            extBuilder = new GenericSearchExtBuilder("unknown");
        }

        return extBuilder;
    }

    @Override
    public String getWriteableName() {
        return EXT_BUILDER_NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(valueType.getValue());
        switch (valueType) {
            case SIMPLE:
                out.writeGenericValue(simpleVal);
                break;
            case MAP:
                out.writeMap(mapVal);
                break;
            case LIST:
                int size = arrayVal.size();
                out.writeVInt(size);
                for (int i = 0; i < size; i++) {
                    out.writeGenericValue(arrayVal.get(i));
                }
                break;
            default:
                throw new IllegalStateException("Unknown valueType: " + valueType);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        switch (valueType) {
            case SIMPLE:
                return builder.field(EXT_BUILDER_NAME.getPreferredName(), simpleVal);
            case MAP:
                return builder.field(EXT_BUILDER_NAME.getPreferredName(), mapVal);
            case LIST:
                return builder.field(EXT_BUILDER_NAME.getPreferredName(), arrayVal);
            default:
                return null;
        }
    }

    // We need this for the equals method.
    Object getValue() {
        switch (valueType) {
            case SIMPLE:
                return this.simpleVal;
            case MAP:
                return Collections.unmodifiableMap(this.mapVal);
            case LIST:
                return Collections.unmodifiableList(this.arrayVal);
            default:
                throw new IllegalStateException("Unknown valueType: " + valueType);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.simpleVal, this.mapVal, this.arrayVal);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof GenericSearchExtBuilder)) {
            return false;
        }
        return Objects.equals(getValue(), ((GenericSearchExtBuilder) obj).getValue());
    }
}
