/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.resetpassword;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to reset an internal user password
 */
public class ResetPasswordRequest extends ActionRequest implements ToXContentObject {

    private String username;
    private String oldPassword;
    private String newPassword;
    private String newPasswordValidation;

    public ResetPasswordRequest(StreamInput in) throws IOException {
        super(in);
        username = in.readString();
        oldPassword = in.readString();
        newPassword = in.readString();
        newPasswordValidation = in.readString();
    }

    public ResetPasswordRequest(String username, String oldPassword, String newPassword, String newPasswordValidation) {
        this.username = username;
        this.oldPassword = oldPassword;
        this.newPassword = newPassword;
        this.newPasswordValidation = newPasswordValidation;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getOldPassword() {
        return oldPassword;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public String getNewPasswordValidation() {
        return newPasswordValidation;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (username == null) { // TODO: check the condition once
            validationException = addValidationError("No username specified", validationException);
        } else if (oldPassword == null) {
            validationException = addValidationError("No current password specified", validationException);
        } else if (newPassword == null) {
            validationException = addValidationError("No new password specified", validationException);
        } else if (newPasswordValidation == null) {
            validationException = addValidationError("No new password verification specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeString(oldPassword);
        out.writeString(newPassword);
        out.writeString(newPasswordValidation);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.value(username);
        builder.value(oldPassword);
        builder.value(newPassword);
        builder.value(newPasswordValidation);
        builder.endObject();
        return builder;
    }
}
