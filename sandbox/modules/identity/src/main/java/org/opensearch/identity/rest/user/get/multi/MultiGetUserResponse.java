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
import org.opensearch.identity.rest.user.get.single.GetUserResponse;
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
    private final List<GetUserResponse> multiGetUserResponses;

    public MultiGetUserResponse(List<GetUserResponse> getUserResponses) {
        this.multiGetUserResponses = getUserResponses;
    }

    public MultiGetUserResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        multiGetUserResponses = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            multiGetUserResponses.add(new GetUserResponse(in));
        }
    }

    public List<GetUserResponse> getMultiGetUserResponses() {
        return multiGetUserResponses;
    }

    @Override
    public RestStatus status() {
        if (multiGetUserResponses.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(multiGetUserResponses.size());
        for (GetUserResponse multiGetUserResponse : multiGetUserResponses) {
            multiGetUserResponse.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("users");
        for (GetUserResponse multiGetUserResponse : multiGetUserResponses) {
            multiGetUserResponse.toXContent(builder, params);
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
            List<GetUserResponse> multiGetUserResponses = (List<GetUserResponse>) parsedObjects[0];
            return new MultiGetUserResponse(multiGetUserResponses);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), GetUserResponse.PARSER, new ParseField("users"));
    }

}
