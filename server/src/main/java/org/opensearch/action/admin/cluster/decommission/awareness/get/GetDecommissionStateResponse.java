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
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response for decommission status
 *
 * @opensearch.internal
 */
public class GetDecommissionStateResponse extends ActionResponse implements ToXContentObject {

    private DecommissionAttribute decommissionedAttribute;
    private DecommissionStatus status;

    GetDecommissionStateResponse() {
        this(null, null);
    }

    GetDecommissionStateResponse(DecommissionAttribute decommissionedAttribute, DecommissionStatus status) {
        this.decommissionedAttribute = decommissionedAttribute;
        this.status = status;
    }

    GetDecommissionStateResponse(StreamInput in) throws IOException {
        // read decommissioned attribute and status only if it is present
        if (in.readBoolean()) {
            this.decommissionedAttribute = new DecommissionAttribute(in);
        }
        if (in.readBoolean()) {
            this.status = DecommissionStatus.fromString(in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // if decommissioned attribute is null, mark absence of decommissioned attribute
        if (decommissionedAttribute == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            decommissionedAttribute.writeTo(out);
        }

        // if status is null, mark absence of status
        if (status == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(status.status());
        }
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
        if (status != null) {
            builder.field("status", status);
        }
        builder.endObject();
        return builder;
    }

    public static GetDecommissionStateResponse fromXContent(XContentParser parser) throws IOException {
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
                            parser.nextToken();
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
                    status = DecommissionStatus.fromString(parser.text().toLowerCase(Locale.ROOT));
                } else {
                    throw new OpenSearchParseException(
                        "unknown field found [{}], failed to parse the decommission attribute",
                        currentFieldName
                    );
                }
            }
        }
        return new GetDecommissionStateResponse(decommissionAttribute, status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetDecommissionStateResponse that = (GetDecommissionStateResponse) o;
        return decommissionedAttribute.equals(that.decommissionedAttribute) && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(decommissionedAttribute, status);
    }
}
