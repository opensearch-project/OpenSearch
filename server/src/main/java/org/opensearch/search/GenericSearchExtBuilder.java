/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This is a catch-all SearchExtBuilder implementation that is used when an appropriate SearchExtBuilder
 * is not found during SearchResponse's fromXContent operation.
 */
public final class GenericSearchExtBuilder extends SearchExtBuilder {

    public final static ParseField EXT_BUILDER_NAME = new ParseField("generic_ext");

    private final Object genericObj;
    private final ValueType valueType;

    enum ValueType {
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

    public GenericSearchExtBuilder(Object genericObj, ValueType valueType) {
        this.genericObj = genericObj;
        this.valueType = valueType;
    }

    public GenericSearchExtBuilder(StreamInput in) throws IOException {
        valueType = ValueType.fromInt(in.readInt());
        switch (valueType) {
            case SIMPLE:
                genericObj = in.readGenericValue();
                break;
            case MAP:
                genericObj = in.readMap();
                break;
            case LIST:
                genericObj = in.readList(r -> r.readGenericValue());
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
        ValueType valueType;
        Object genericObj;
        if (token == XContentParser.Token.START_OBJECT) {
            genericObj = parser.map();
            valueType = ValueType.MAP;
        } else if (token == XContentParser.Token.START_ARRAY) {
            genericObj = parser.list();
            valueType = ValueType.LIST;
        } else if (token.isValue()) {
            genericObj = parser.objectText();
            valueType = ValueType.SIMPLE;
        } else {
            throw new XContentParseException("Unknown token: " + token);
        }

        return new GenericSearchExtBuilder(genericObj, valueType);
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
                out.writeGenericValue(genericObj);
                break;
            case MAP:
                out.writeMap((Map<String, Object>) genericObj);
                break;
            case LIST:
                out.writeCollection((List<Object>) genericObj, StreamOutput::writeGenericValue);
                break;
            default:
                throw new IllegalStateException("Unknown valueType: " + valueType);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        switch (valueType) {
            case SIMPLE:
                return builder.field(EXT_BUILDER_NAME.getPreferredName(), genericObj);
            case MAP:
                return builder.field(EXT_BUILDER_NAME.getPreferredName(), (Map<String, Object>) genericObj);
            case LIST:
                return builder.field(EXT_BUILDER_NAME.getPreferredName(), (List<Object>) genericObj);
            default:
                return null;
        }
    }

    // We need this for the equals method.
    Object getValue() {
        return genericObj;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.valueType, this.genericObj);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof GenericSearchExtBuilder)) {
            return false;
        }
        return Objects.equals(getValue(), ((GenericSearchExtBuilder) obj).getValue())
            && Objects.equals(valueType, ((GenericSearchExtBuilder) obj).valueType);
    }
}
