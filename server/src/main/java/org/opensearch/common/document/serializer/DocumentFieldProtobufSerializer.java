/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.document.serializer;

import com.google.protobuf.ByteString;
import org.opensearch.OpenSearchException;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.text.Text;
import org.opensearch.server.proto.FetchSearchResultProto;
import org.opensearch.server.proto.FetchSearchResultProto.DocumentFieldValue;
import org.opensearch.server.proto.FetchSearchResultProto.DocumentFieldValue.Builder;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializer for {@link DocumentField} to/from protobuf.
 */
public class DocumentFieldProtobufSerializer implements DocumentFieldSerializer<InputStream> {

    private FetchSearchResultProto.SearchHit.DocumentField documentField;

    @Override
    public DocumentField createDocumentField(InputStream inputStream) throws IOException {
        documentField = FetchSearchResultProto.SearchHit.DocumentField.parseFrom(inputStream);
        String name = documentField.getName();
        List<Object> values = new ArrayList<>();
        for (FetchSearchResultProto.DocumentFieldValue value : documentField.getValuesList()) {
            values.add(readDocumentFieldValueFromProtobuf(value));
        }
        return new DocumentField(name, values);
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

    public static DocumentFieldValue.Builder convertDocumentFieldValueToProto(Object value, Builder valueBuilder) {
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

    public static FetchSearchResultProto.SearchHit.DocumentField convertDocumentFieldToProto(DocumentField documentField) {
        FetchSearchResultProto.SearchHit.DocumentField.Builder builder = FetchSearchResultProto.SearchHit.DocumentField.newBuilder();
        builder.setName(documentField.getName());
        for (Object value : documentField.getValues()) {
            FetchSearchResultProto.DocumentFieldValue.Builder valueBuilder = FetchSearchResultProto.DocumentFieldValue.newBuilder();
            builder.addValues(DocumentFieldProtobufSerializer.convertDocumentFieldValueToProto(value, valueBuilder));
        }
        return builder.build();
    }

}
