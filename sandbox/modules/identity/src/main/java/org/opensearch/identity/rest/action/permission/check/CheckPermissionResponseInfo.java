/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.check;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class CheckPermissionResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final List<String> permissionStrings;

    public CheckPermissionResponseInfo(boolean successful, List<String> permissionStrings) {
        this.successful = successful;
        this.permissionStrings = permissionStrings;
    }

    public CheckPermissionResponseInfo(StreamInput in) throws IOException {

        successful = in.readBoolean();
        permissionStrings = in.readStringList();
    }

    public boolean isSuccessful() {
        return successful;
    }

    public List<String> getPermissionString() {
        return permissionStrings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeStringArray((String[]) permissionStrings.toArray());
    }

    static final ConstructingObjectParser<CheckPermissionResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "check_permission_response_info",
        true,
        args -> new CheckPermissionResponseInfo((boolean) args[0], (List<String>) args[1])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareStringArray(constructorArg(), new ParseField("permissionStrings"));
        PARSER.declareString(constructorArg(), new ParseField("message"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField PERMISSION_STRING = new ParseField("permissionStrings");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), successful);
        builder.field(PERMISSION_STRING.getPreferredName(), permissionStrings);
        builder.endObject();
        return builder;
    }
}
