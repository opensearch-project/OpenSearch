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
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Rest response class that contains aggregated list of delete user actions
 */
public class DeleteUserResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final List<DeleteUserResponseInfo> deleteUserResponseInfo;

    public DeleteUserResponse(List<DeleteUserResponseInfo> deleteUserResponseInfo) {
        this.deleteUserResponseInfo = deleteUserResponseInfo;
    }

    public DeleteUserResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        deleteUserResponseInfo = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            deleteUserResponseInfo.add(new DeleteUserResponseInfo(in));
        }

    }

    public List<DeleteUserResponseInfo> getDeleteUserResponseInfo() {
        return deleteUserResponseInfo;
    }

    @Override
    public RestStatus status() {
        if (deleteUserResponseInfo.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(deleteUserResponseInfo.size());
        for (DeleteUserResponseInfo createUserResults : deleteUserResponseInfo) {
            createUserResults.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("users");
        for (DeleteUserResponseInfo response : deleteUserResponseInfo) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<DeleteUserResponse, Void> PARSER = new ConstructingObjectParser<>(
        "delete_user_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<DeleteUserResponseInfo> createUserResponseInfoList = (List<DeleteUserResponseInfo>) parsedObjects[0];
            return new DeleteUserResponse(createUserResponseInfoList);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), DeleteUserResponseInfo.PARSER, new ParseField("users"));
    }

}
