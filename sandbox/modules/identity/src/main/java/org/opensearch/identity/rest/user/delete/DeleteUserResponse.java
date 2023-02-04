/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.delete;

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
 * Rest response class that contains aggregated list of delete user actions
 */
public class DeleteUserResponse extends ActionResponse implements StatusToXContentObject {

    private final DeleteUserResponseInfo deleteUserResponseInfo;

    public DeleteUserResponse(DeleteUserResponseInfo deleteUserResponseInfo) {
        this.deleteUserResponseInfo = deleteUserResponseInfo;
    }

    public DeleteUserResponse(StreamInput in) throws IOException {
        super(in);
        deleteUserResponseInfo = new DeleteUserResponseInfo(in);
    }

    public DeleteUserResponseInfo getDeleteUserResponseInfo() {
        return deleteUserResponseInfo;
    }

    @Override
    public RestStatus status() {
        if (deleteUserResponseInfo == null) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (deleteUserResponseInfo != null) deleteUserResponseInfo.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (deleteUserResponseInfo != null) deleteUserResponseInfo.toXContent(builder, params);
        return builder;
    }

    private static final ConstructingObjectParser<DeleteUserResponse, Void> PARSER = new ConstructingObjectParser<>(
        "delete_user_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            DeleteUserResponseInfo deleteUserResponseInfoList = (DeleteUserResponseInfo) parsedObjects[0];
            return new DeleteUserResponse(deleteUserResponseInfoList);
        }
    );
    static {
        PARSER.declareObject(constructorArg(), DeleteUserResponseInfo.PARSER, new ParseField("user"));
    }

}
