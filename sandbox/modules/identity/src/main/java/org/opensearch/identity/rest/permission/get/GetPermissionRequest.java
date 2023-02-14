/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.get;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

public class GetPermissionRequest extends ActionRequest implements ToXContentObject {

    private String username;

    public GetPermissionRequest(StreamInput in) throws IOException {
        super(in);
        username = in.readString();
    }

    public GetPermissionRequest(String username) {
        this.username = username;
    }

    public GetPermissionRequest() {}

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    /**
     * Ensure that the username is present
     * @return Returns an exception on invalid request content
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (this.username == null) { // TODO: check the condition once
            validationException = addValidationError("No principal provided.", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.value(username);
        builder.endObject();
        return builder;
    }
}
