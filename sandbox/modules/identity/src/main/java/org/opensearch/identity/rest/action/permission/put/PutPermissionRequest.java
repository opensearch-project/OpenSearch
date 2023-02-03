/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.put;

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

/**
 * This class defines a PutPermissionAction request
 */
public class PutPermissionRequest extends ActionRequest implements ToXContentObject {

    private String permissionString;

    private String username;

    public PutPermissionRequest(StreamInput in) throws IOException {
        super(in);
        this.permissionString = in.readString();
        this.username = in.readString();
    }

    public PutPermissionRequest(String permissionString, String username) {
        this.permissionString = permissionString;
        this.username = username;
    }

    public PutPermissionRequest() {}

    public String getPermissionString() {
        return this.permissionString;
    }

    public void setPermissionString(String permissionString) {
        this.permissionString = permissionString;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Ensure that both the permission and username are present
     * @return
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (permissionString == null) { // TODO: check the condition once
            validationException = addValidationError("No permissionString specified", validationException);
        } else if (username == null) {
            validationException = addValidationError("No username specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(permissionString);
        out.writeString(username);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.value(permissionString);
        builder.value(username);
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException { // TODO: Talk to DC about this

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("permissionString".equals(currentFieldName)) {

                    if (token.isValue() == false) {
                        throw new IllegalArgumentException(); // TODO: check the message to be returned
                    }

                    // add any validation checks here

                    permissionString = parser.text();
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
