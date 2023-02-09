/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.put;

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

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Response class for create user request
 * Contains list of responses of each user creation request
 */
public class PutUserResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final PutUserResponseInfo putUserResponseInfo;

    public PutUserResponse(PutUserResponseInfo putUserResponseInfo) {
        this.putUserResponseInfo = putUserResponseInfo;
    }

    public PutUserResponse(StreamInput in) throws IOException {
        super(in);
        putUserResponseInfo = new PutUserResponseInfo(in);
    }

    public PutUserResponseInfo getPutUserResponseInfo() {
        return putUserResponseInfo;
    }

    /**
     * @return Whether the attempt to Create a user was successful
     */
    @Override
    public RestStatus status() {
        if (putUserResponseInfo == null) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (putUserResponseInfo != null) {
            putUserResponseInfo.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (putUserResponseInfo != null) {
            putUserResponseInfo.toXContent(builder, params);
        }
        return builder;
    }

    private static final ConstructingObjectParser<PutUserResponse, Void> PARSER = new ConstructingObjectParser<>(
        "put_user_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            PutUserResponseInfo putUserResponseInfo1 = (PutUserResponseInfo) parsedObjects[0];
            return new PutUserResponse(putUserResponseInfo1);
        }
    );
    static {
        PARSER.declareObject(constructorArg(), PutUserResponseInfo.PARSER, new ParseField("user"));
    }

    public static PutUserResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
