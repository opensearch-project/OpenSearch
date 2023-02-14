/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.delete;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * This class defines a DeletePermissionAction request
 */
public class DeletePermissionRequest extends ActionRequest implements ToXContentObject {

    private String username;
    private String permissionString;

    public DeletePermissionRequest(StreamInput in) throws IOException {
        super(in);
        this.username = in.readString();
        this.permissionString = in.readString();
    }

    public DeletePermissionRequest(String username, String permissionString) {
        this.username = username;
        this.permissionString = permissionString;

    }

    public DeletePermissionRequest() {}

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPermissionString() {
        return this.permissionString;
    }

    public void setPermissionString(String permissionString) {
        this.permissionString = permissionString;
    }

    /**
     * Ensure that both the permission and username are present
     * @return Returns an exception on invalid request content
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (username == null) {
            validationException = addValidationError("No username specified", validationException);
        } else if (permissionString == null) { // TODO: check the condition once
            validationException = addValidationError("No permissionString specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeString(permissionString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.value(username);
        builder.value(permissionString);
        builder.endObject();
        return builder;
    }
}
