/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.add;

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
 * Response info corresponds to a single response for a permission add action
 */
public class AddPermissionResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String permissionString;
    private final String principalString;

    public AddPermissionResponseInfo(boolean successful, String permissionString, String principalString) {
        this.successful = successful;
        this.permissionString = permissionString;
        this.principalString = principalString;
    }

    public AddPermissionResponseInfo(StreamInput in) throws IOException {
        this.successful = in.readBoolean();
        this.permissionString = in.readString();
        this.principalString = in.readString();

    }

    public String getPermissionString() {
        return permissionString;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(this.successful);
        out.writeString(this.permissionString);
        out.writeString(this.principalString);
    }

    static final ConstructingObjectParser<AddPermissionResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "add_permission_response_info",
        true,
        args -> new AddPermissionResponseInfo((boolean) args[0], (String) args[1], (String) args[2])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("permissionString"));
        PARSER.declareString(constructorArg(), new ParseField("principalString"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField PERMISSION_STRING = new ParseField("permissionString");
    private static final ParseField PRINCIPAL_STRING = new ParseField("principalString");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), this.successful);
        builder.field(PERMISSION_STRING.getPreferredName(), this.permissionString);
        builder.field(PRINCIPAL_STRING.getPreferredName(), this.principalString);
        builder.endObject();
        return builder;
    }
}
