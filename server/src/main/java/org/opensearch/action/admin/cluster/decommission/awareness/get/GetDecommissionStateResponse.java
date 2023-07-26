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
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response for decommission status
 *
 * @opensearch.internal
 */
public class GetDecommissionStateResponse extends ActionResponse implements ToXContentObject {

    private String attributeValue;
    private DecommissionStatus status;

    GetDecommissionStateResponse() {
        this(null, null);
    }

    GetDecommissionStateResponse(String attributeValue, DecommissionStatus status) {
        this.attributeValue = attributeValue;
        this.status = status;
    }

    GetDecommissionStateResponse(StreamInput in) throws IOException {
        // read decommissioned attribute and status only if it is present
        if (in.readBoolean()) {
            this.attributeValue = in.readString();
            this.status = DecommissionStatus.fromString(in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // if decommissioned attribute value is null or status is null then mark its absence
        if (attributeValue == null || status == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(attributeValue);
            out.writeString(status.status());
        }
    }

    public String getAttributeValue() {
        return attributeValue;
    }

    public DecommissionStatus getDecommissionStatus() {
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (attributeValue != null && status != null) {
            builder.field(attributeValue, status);
        }
        builder.endObject();
        return builder;
    }

    public static GetDecommissionStateResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        String attributeValue = null;
        DecommissionStatus status = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                attributeValue = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                    throw new OpenSearchParseException("failed to parse status of decommissioning, expected string but found unknown type");
                }
                status = DecommissionStatus.fromString(parser.text().toLowerCase(Locale.ROOT));
            } else {
                throw new OpenSearchParseException(
                    "failed to parse decommission state, expected [{}] but found [{}]",
                    XContentParser.Token.FIELD_NAME,
                    token
                );
            }
        }
        return new GetDecommissionStateResponse(attributeValue, status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetDecommissionStateResponse that = (GetDecommissionStateResponse) o;
        if (!Objects.equals(attributeValue, that.attributeValue)) {
            return false;
        }
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeValue, status);
    }
}
