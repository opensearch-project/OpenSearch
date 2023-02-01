/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.check;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

public class CheckPermissionRequest extends ActionRequest implements ToXContentObject {

    private String principalString;

    public CheckPermissionRequest(StreamInput in) throws IOException {
        super(in);
        principalString = in.readString();
    }

    public CheckPermissionRequest(String principalString) {
        this.principalString = principalString;
    }

    public CheckPermissionRequest() {}

    public void setPrincipalString(String principalString) {
        this.principalString = principalString;
    }

    public String getPrincipalString() {
        return principalString;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (this.principalString == null) { // TODO: check the condition once
            validationException = addValidationError("No principal provided.", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(principalString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.value(principalString);
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("principalString".equals(currentFieldName)) {

                    if (token.isValue() == false) {
                        throw new IllegalArgumentException(); // TODO: check the message to be returned
                    }

                    // add any validation checks here

                    principalString = parser.text();

                }
                // add all other fields to be parsed from body
                else {
                    throw new IllegalArgumentException(
                        "Unknown parameter [" + currentFieldName + "] in request body or parameter is of the wrong type[" + token + "] "
                    );
                }
            }
        }
    }
}
