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

package org.opensearch.common.document;

import com.google.protobuf.ByteString;
import org.opensearch.OpenSearchException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.get.GetResult;
import org.opensearch.search.SearchHit;
import org.opensearch.server.proto.FetchSearchResultProto;
import org.opensearch.server.proto.FetchSearchResultProto.DocumentFieldValue;
import org.opensearch.server.proto.FetchSearchResultProto.DocumentFieldValue.Builder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.parseFieldsValue;

/**
 * A single field name and values part of {@link SearchHit} and {@link GetResult}.
 *
 * @see SearchHit
 * @see GetResult
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DocumentField implements Writeable, ToXContentFragment, Iterable<Object> {

    private final String name;
    private final List<Object> values;
    private FetchSearchResultProto.SearchHit.DocumentField documentField;

    public DocumentField(StreamInput in) throws IOException {
        name = in.readString();
        values = in.readList(StreamInput::readGenericValue);
    }

    public DocumentField(String name, List<Object> values) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.values = Objects.requireNonNull(values, "values must not be null");
    }

    public DocumentField(byte[] in) throws IOException {
        assert FeatureFlags.isEnabled(FeatureFlags.PROTOBUF) : "protobuf feature flag is not enabled";
        documentField = FetchSearchResultProto.SearchHit.DocumentField.parseFrom(in);
        name = documentField.getName();
        values = new ArrayList<>();
        for (FetchSearchResultProto.DocumentFieldValue value : documentField.getValuesList()) {
            values.add(readDocumentFieldValueFromProtobuf(value));
        }
    }

    public static FetchSearchResultProto.SearchHit.DocumentField convertDocumentFieldToProto(DocumentField documentField) {
        FetchSearchResultProto.SearchHit.DocumentField.Builder builder = FetchSearchResultProto.SearchHit.DocumentField.newBuilder();
        builder.setName(documentField.getName());
        for (Object value : documentField.getValues()) {
            FetchSearchResultProto.DocumentFieldValue.Builder valueBuilder = FetchSearchResultProto.DocumentFieldValue.newBuilder();
            builder.addValues(convertDocumentFieldValueToProto(value, valueBuilder));
        }
        return builder.build();
    }

    private static DocumentFieldValue.Builder convertDocumentFieldValueToProto(Object value, Builder valueBuilder) {
        if (value == null) {
            // null is not allowed in protobuf, so we use a special string to represent null
            return valueBuilder.setValueString("null");
        }
        Class type = value.getClass();
        if (type == String.class) {
            valueBuilder.setValueString((String) value);
        } else if (type == Integer.class) {
            valueBuilder.setValueInt((Integer) value);
        } else if (type == Long.class) {
            valueBuilder.setValueLong((Long) value);
        } else if (type == Float.class) {
            valueBuilder.setValueFloat((Float) value);
        } else if (type == Double.class) {
            valueBuilder.setValueDouble((Double) value);
        } else if (type == Boolean.class) {
            valueBuilder.setValueBool((Boolean) value);
        } else if (type == byte[].class) {
            valueBuilder.addValueByteArray(ByteString.copyFrom((byte[]) value));
        } else if (type == List.class) {
            List<Object> list = (List<Object>) value;
            for (Object listValue : list) {
                valueBuilder.addValueArrayList(convertDocumentFieldValueToProto(listValue, valueBuilder));
            }
        } else if (type == Map.class || type == HashMap.class || type == LinkedHashMap.class) {
            Map<String, Object> map = (Map<String, Object>) value;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                valueBuilder.putValueMap(entry.getKey(), convertDocumentFieldValueToProto(entry.getValue(), valueBuilder).build());
            }
        } else if (type == Date.class) {
            valueBuilder.setValueDate(((Date) value).getTime());
        } else if (type == ZonedDateTime.class) {
            valueBuilder.setValueZonedDate(((ZonedDateTime) value).getZone().getId());
            valueBuilder.setValueZonedTime(((ZonedDateTime) value).toInstant().toEpochMilli());
        } else if (type == Text.class) {
            valueBuilder.setValueText(((Text) value).string());
        } else {
            throw new OpenSearchException("Can't convert generic value of type [" + type + "] to protobuf");
        }
        return valueBuilder;
    }

    private Object readDocumentFieldValueFromProtobuf(FetchSearchResultProto.DocumentFieldValue documentFieldValue) throws IOException {
        if (documentFieldValue.hasValueString()) {
            return documentFieldValue.getValueString();
        } else if (documentFieldValue.hasValueInt()) {
            return documentFieldValue.getValueInt();
        } else if (documentFieldValue.hasValueLong()) {
            return documentFieldValue.getValueLong();
        } else if (documentFieldValue.hasValueFloat()) {
            return documentFieldValue.getValueFloat();
        } else if (documentFieldValue.hasValueDouble()) {
            return documentFieldValue.getValueDouble();
        } else if (documentFieldValue.hasValueBool()) {
            return documentFieldValue.getValueBool();
        } else if (documentFieldValue.getValueByteArrayList().size() > 0) {
            return documentFieldValue.getValueByteArrayList().toArray();
        } else if (documentFieldValue.getValueArrayListList().size() > 0) {
            List<Object> list = new ArrayList<>();
            for (FetchSearchResultProto.DocumentFieldValue value : documentFieldValue.getValueArrayListList()) {
                list.add(readDocumentFieldValueFromProtobuf(value));
            }
            return list;
        } else if (documentFieldValue.getValueMapMap().size() > 0) {
            Map<String, Object> map = Map.of();
            for (Map.Entry<String, FetchSearchResultProto.DocumentFieldValue> entrySet : documentFieldValue.getValueMapMap().entrySet()) {
                map.put(entrySet.getKey(), readDocumentFieldValueFromProtobuf(entrySet.getValue()));
            }
            return map;
        } else if (documentFieldValue.hasValueDate()) {
            return new Date(documentFieldValue.getValueDate());
        } else if (documentFieldValue.hasValueZonedDate() && documentFieldValue.hasValueZonedTime()) {
            return ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(documentFieldValue.getValueZonedTime()),
                ZoneId.of(documentFieldValue.getValueZonedDate())
            );
        } else if (documentFieldValue.hasValueText()) {
            return new Text(documentFieldValue.getValueText());
        } else {
            throw new IOException("Can't read generic value of type [" + documentFieldValue + "]");
        }
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * The first value of the hit.
     */
    public <V> V getValue() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return (V) values.get(0);
    }

    /**
     * The field values.
     */
    public List<Object> getValues() {
        return values;
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(values, StreamOutput::writeGenericValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(name);
        for (Object value : values) {
            // This call doesn't really need to support writing any kind of object, since the values
            // here are always serializable to xContent. Each value could be a leaf types like a string,
            // number, or boolean, a list of such values, or a map of such values with string keys.
            builder.value(value);
        }
        builder.endArray();
        return builder;
    }

    public static DocumentField fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
        List<Object> values = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            values.add(parseFieldsValue(parser));
        }
        return new DocumentField(fieldName, values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DocumentField objects = (DocumentField) o;
        return Objects.equals(name, objects.name) && Objects.equals(values, objects.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values);
    }

    @Override
    public String toString() {
        return "DocumentField{" + "name='" + name + '\'' + ", values=" + values + '}';
    }
}
