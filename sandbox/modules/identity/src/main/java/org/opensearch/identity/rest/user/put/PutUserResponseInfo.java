/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.put;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class captures if creation of a single given user is successful
 * If so, returns a boolean flag along-with the provided username
 */
public class PutUserResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String username;

    private final String message;

    public PutUserResponseInfo(boolean successful, String username, String message) {
        this.successful = successful;
        this.username = username;
        this.message = message;
    }

    public PutUserResponseInfo(StreamInput in) throws IOException {
        successful = in.readBoolean();
        username = in.readString();
        message = in.readString();
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getUsername() {
        return username;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeString(username);
        out.writeString(message);
    }

    static final ConstructingObjectParser<PutUserResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "put_user_response_info",
        true,
        args -> new PutUserResponseInfo((boolean) args[0], (String) args[1], (String) args[2])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareString(constructorArg(), new ParseField("message"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField USERNAME = new ParseField("username");
    private static final ParseField MESSAGE = new ParseField("message");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), successful);
        builder.field(USERNAME.getPreferredName(), username);
        builder.field(MESSAGE.getPreferredName(), message);
        builder.endObject();
        return builder;
    }

}
