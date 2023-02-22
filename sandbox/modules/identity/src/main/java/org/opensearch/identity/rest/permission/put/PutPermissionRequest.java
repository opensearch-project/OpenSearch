/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.put;

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
 * This class defines a PutPermissionAction request
 */
public class PutPermissionRequest extends ActionRequest implements ToXContentObject {

    private String username;
    private String permission;

    public PutPermissionRequest(StreamInput in) throws IOException {
        super(in);
        this.username = in.readString();
        this.permission = in.readString();
    }

    public PutPermissionRequest(String username, String permission) {
        this.username = username;
        this.permission = permission;

    }

    public PutPermissionRequest() {}

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPermission() {
        return this.permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
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
        } else if (permission == null) { // TODO: check the condition once
            validationException = addValidationError("No permission specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeString(permission);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.value(username);
        builder.value(permission);
        builder.endObject();
        return builder;
    }
}
