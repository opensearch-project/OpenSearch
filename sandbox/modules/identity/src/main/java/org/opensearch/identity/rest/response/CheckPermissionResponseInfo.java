/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.response;

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

public class CheckPermissionResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String permissionString;

    public CheckPermissionResponseInfo(boolean successful, String permissionString) {
        this.successful = successful;
        this.permissionString = permissionString;
    }

    public CheckPermissionResponseInfo(StreamInput in) throws IOException {
        successful = in.readBoolean();
        permissionString = in.readString();

    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getPermissionString() {
        return permissionString;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeString(permissionString);
    }

    static final ConstructingObjectParser<CheckPermissionResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "check_permission_response_info",
        true,
        args -> new CheckPermissionResponseInfo((boolean) args[0], (String) args[1])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("permission_string"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField PERMISSION_STRING = new ParseField("permission_string");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), successful);
        builder.field(PERMISSION_STRING.getPreferredName(), permissionString);
        builder.endObject();
        return builder;
    }
}
