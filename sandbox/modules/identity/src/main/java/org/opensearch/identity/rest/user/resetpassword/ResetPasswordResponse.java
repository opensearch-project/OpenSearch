/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.resetpassword;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Response class for create user request
 * Contains list of responses of each user creation request
 */
public class ResetPasswordResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final ResetPasswordResponseInfo resetPasswordResponseInfo;

    public ResetPasswordResponse(ResetPasswordResponseInfo resetPasswordResponseInfo) {
        this.resetPasswordResponseInfo = resetPasswordResponseInfo;
    }

    public ResetPasswordResponse(StreamInput in) throws IOException {
        super(in);
        resetPasswordResponseInfo = new ResetPasswordResponseInfo(in);
    }

    public ResetPasswordResponseInfo getResetPasswordResponseInfo() {
        return resetPasswordResponseInfo;
    }

    /**
     * @return Whether the attempt to Create a user was successful
     */
    @Override
    public RestStatus status() {
        if (resetPasswordResponseInfo == null) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (resetPasswordResponseInfo != null) {
            resetPasswordResponseInfo.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (resetPasswordResponseInfo != null) {
            resetPasswordResponseInfo.toXContent(builder, params);
        }
        return builder;
    }

    private static final ConstructingObjectParser<ResetPasswordResponse, Void> PARSER = new ConstructingObjectParser<>(
        "reset_password_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            ResetPasswordResponseInfo resetPasswordResponseInfo1 = (ResetPasswordResponseInfo) parsedObjects[0];
            return new ResetPasswordResponse(resetPasswordResponseInfo1);
        }
    );
    static {
        PARSER.declareObject(constructorArg(), ResetPasswordResponseInfo.PARSER, new ParseField("user"));
    }

}
