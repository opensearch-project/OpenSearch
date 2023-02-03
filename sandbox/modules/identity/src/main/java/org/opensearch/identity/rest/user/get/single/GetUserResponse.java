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
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Rest response class for the requested user
 */
public class GetUserResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final GetUserResponseInfo getUserResponseInfo;

    public GetUserResponse(GetUserResponseInfo getUserResponseInfo) {
        this.getUserResponseInfo = getUserResponseInfo;
    }

    public GetUserResponse(StreamInput in) throws IOException {
        super(in);
        // TODO: if making GetUserResponseInfo is a better way to handle this
        //  (i.e. use readNamedWriteable() and writeNamedWritable())
        getUserResponseInfo = new GetUserResponseInfo(
            in.readString(),
            in.readMap(StreamInput::readString, StreamInput::readString),
            in.readList(StreamInput::readString)
        );

    }

    public GetUserResponseInfo getGetUserResponseInfo() {
        return getUserResponseInfo;
    }

    @Override
    public RestStatus status() {
        if (getUserResponseInfo == null) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        getUserResponseInfo.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("user");
        getUserResponseInfo.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<GetUserResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_user_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            GetUserResponseInfo getUserResponseInfo = (GetUserResponseInfo) parsedObjects[0];
            return new GetUserResponse(getUserResponseInfo);
        }
    );
    static {
        PARSER.declareObject(constructorArg(), GetUserResponseInfo.PARSER, new ParseField("user"));
    }

}
