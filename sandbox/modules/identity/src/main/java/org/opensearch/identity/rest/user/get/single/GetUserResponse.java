/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.get.single;

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
 * Rest response class for the requested user
 */
public class GetUserResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final String username;
    private final GetUserResponseInfo getUserResponseInfo;

    public GetUserResponse(String username, GetUserResponseInfo getUserResponseInfo) {
        this.username = username;
        this.getUserResponseInfo = getUserResponseInfo;
    }

    public GetUserResponse() {
        this.username = null;
        this.getUserResponseInfo = null;
    }

    public GetUserResponse(StreamInput in) throws IOException {
        super(in);
        this.username = in.readString();
        getUserResponseInfo = new GetUserResponseInfo(in);
    }

    public String getUsername() { return username; }
    public GetUserResponseInfo getGetUserResponseInfo() {
        return getUserResponseInfo;
    }

    @Override
    public RestStatus status() {
        if (username == null || getUserResponseInfo == null) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(username);
        if (getUserResponseInfo != null) getUserResponseInfo.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(username);
        if (getUserResponseInfo != null) getUserResponseInfo.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static final ConstructingObjectParser<GetUserResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_user_response",
        true,
        (Object[] parsedObjects) -> {
            String username = (String) parsedObjects[0];
            @SuppressWarnings("unchecked")
            GetUserResponseInfo getUserResponseInfo = (GetUserResponseInfo) parsedObjects[1];
            return new GetUserResponse(username, getUserResponseInfo);
        }
    );
    static {
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareObject(constructorArg(), GetUserResponseInfo.PARSER, new ParseField("user"));
    }

}
