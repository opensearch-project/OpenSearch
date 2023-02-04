/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.get.multi;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.identity.rest.user.get.single.GetUserResponseInfo;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Response class the contains list of multiple users
 */
public class MultiGetUserResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final List<GetUserResponseInfo> multiGetUserResponseInfo;

    public MultiGetUserResponse(List<GetUserResponseInfo> createUserResults) {
        this.multiGetUserResponseInfo = createUserResults;
    }

    public MultiGetUserResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        multiGetUserResponseInfo = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            multiGetUserResponseInfo.add(new GetUserResponseInfo(in));
        }
    }

    public List<GetUserResponseInfo> getMultiGetUserResponseInfo() {
        return multiGetUserResponseInfo;
    }

    @Override
    public RestStatus status() {
        if (multiGetUserResponseInfo.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(multiGetUserResponseInfo.size());
        for (GetUserResponseInfo multiGetUserResponseInfo : multiGetUserResponseInfo) {
            multiGetUserResponseInfo.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("users");
        for (GetUserResponseInfo multiGetUserResponseInfo : multiGetUserResponseInfo) {
            multiGetUserResponseInfo.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<MultiGetUserResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_users_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<GetUserResponseInfo> multiGetUserResponseInfo = (List<GetUserResponseInfo>) parsedObjects[0];
            return new MultiGetUserResponse(multiGetUserResponseInfo);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), GetUserResponseInfo.PARSER, new ParseField("users"));
    }

}
