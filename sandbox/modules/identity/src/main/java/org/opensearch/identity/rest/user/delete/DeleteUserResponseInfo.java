/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.delete;

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
 * Transport response for deletion of a single user
 */
public class DeleteUserResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String message;

    public DeleteUserResponseInfo(boolean successful, String message) {
        this.successful = successful;
        this.message = message;
    }

    public DeleteUserResponseInfo(StreamInput in) throws IOException {
        successful = in.readBoolean();
        message = in.readString();
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeString(message);
    }

    static final ConstructingObjectParser<DeleteUserResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "delete_user_response_info",
        true,
        args -> new DeleteUserResponseInfo((boolean) args[0], (String) args[1])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("message"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField USERNAME = new ParseField("message");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), successful);
        builder.field(USERNAME.getPreferredName(), message);
        builder.endObject();
        return builder;
    }

}
