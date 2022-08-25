/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.get;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetDecommissionResponse extends ActionResponse implements ToXContentObject {

    private final DecommissionAttribute decommissionedAttribute;
    private final DecommissionStatus status;

    GetDecommissionResponse(DecommissionAttribute decommissionedAttribute, DecommissionStatus status) {
        this.decommissionedAttribute = decommissionedAttribute;
        this.status = status;
    }

    GetDecommissionResponse(StreamInput in) throws IOException {
        this.decommissionedAttribute = new DecommissionAttribute(in);
        this.status = DecommissionStatus.fromValue(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        decommissionedAttribute.writeTo(out);
        out.writeByte(status.value());
    }

    public DecommissionAttribute getDecommissionedAttribute() {
        return decommissionedAttribute;
    }

    public DecommissionStatus getDecommissionStatus() {
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("awareness");
        if (decommissionedAttribute != null) {
            builder.field(decommissionedAttribute.attributeName(), decommissionedAttribute.attributeValue());
        }
        builder.endObject();
        if (status!=null) {
            builder.field("status", status);
        }
        builder.endObject();
        return builder;
    }

    public static GetDecommissionResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        String attributeType = "awareness";
        XContentParser.Token token;
        DecommissionAttribute decommissionAttribute = null;
        DecommissionStatus status = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (attributeType.equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new OpenSearchParseException(
                            "failed to parse decommission attribute type [{}], expected object",
                            attributeType
                        );
                    }
                    token = parser.nextToken();
                    if (token != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            String fieldName = parser.currentName();
                            String value;
                            token = parser.nextToken();
                            if (token == XContentParser.Token.VALUE_STRING) {
                                value = parser.text();
                            } else {
                                throw new OpenSearchParseException(
                                    "failed to parse attribute [{}], expected string for attribute value",
                                    fieldName
                                );
                            }
                            decommissionAttribute = new DecommissionAttribute(fieldName, value);
                            token = parser.nextToken();
                        } else {
                            throw new OpenSearchParseException("failed to parse attribute type [{}], unexpected type", attributeType);
                        }
                    } else {
                        throw new OpenSearchParseException("failed to parse attribute type [{}]", attributeType);
                    }
                } else if ("status".equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new OpenSearchParseException(
                            "failed to parse status of decommissioning, expected string but found unknown type"
                        );
                    }
                    status = DecommissionStatus.fromString(parser.text());
                } else {
                    throw new OpenSearchParseException(
                        "unknown field found [{}], failed to parse the decommission attribute",
                        currentFieldName
                    );
                }
            }
        }
        return new GetDecommissionResponse(decommissionAttribute, status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetDecommissionResponse that = (GetDecommissionResponse) o;
        return decommissionedAttribute.equals(that.decommissionedAttribute) && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(decommissionedAttribute, status);
    }
}
