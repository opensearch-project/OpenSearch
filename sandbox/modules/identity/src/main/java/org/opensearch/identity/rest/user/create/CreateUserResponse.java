/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.create;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.ToXContent;
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
 * Response class for create user request
 * Contains list of responses of each user creation request
 */
public class CreateUserResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final List<CreateUserResponseInfo> createUserResults;

    public CreateUserResponse(List<CreateUserResponseInfo> createUserResults) {
        this.createUserResults = createUserResults;
    }

    public CreateUserResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        createUserResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            createUserResults.add(new CreateUserResponseInfo(in));
        }

    }

    public List<CreateUserResponseInfo> getCreateUserResults() {
        return createUserResults;
    }

    /**
     * @return Whether the attempt to Create a user was successful
     */
    @Override
    public RestStatus status() {
        if (createUserResults.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(createUserResults.size());
        for (CreateUserResponseInfo createUserResults : createUserResults) {
            createUserResults.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("users");
        for (CreateUserResponseInfo response : createUserResults) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<CreateUserResponse, Void> PARSER = new ConstructingObjectParser<>(
        "create_user_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<CreateUserResponseInfo> createUserResponseInfoList = (List<CreateUserResponseInfo>) parsedObjects[0];
            return new CreateUserResponse(createUserResponseInfoList);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), CreateUserResponseInfo.PARSER, new ParseField("users"));
    }

    public static CreateUserResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
