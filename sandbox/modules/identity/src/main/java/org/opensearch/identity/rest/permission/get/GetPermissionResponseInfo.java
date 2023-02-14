/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.get;

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

/**
 * Response info corresponds to a single response for a permission get action
 */
public class GetPermissionResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String username;
    private final List<String> permissionList;

    /**
     * Construct an instance of a get permission response info (these are aggregated into a response for bulk requests)
     * @param successful whether the request was successful
     * @param username the username of the principal having its permissions checked
     * @param permissionList the permission string to be granted
     */
    public GetPermissionResponseInfo(boolean successful, String username, List<String> permissionList) {
        this.successful = successful;
        this.username = username;
        this.permissionList = permissionList;
    }

    /**
     * A stream based constructor
     * @param in an input stream for dealing with input from another node
     * @throws IOException Throw on failure
     */
    public GetPermissionResponseInfo(StreamInput in) throws IOException {
        this.successful = in.readBoolean();
        this.username = in.readString();
        this.permissionList = in.readStringList();
    }

    public boolean isSuccessful() {
        return this.successful;
    }

    public String getUsername() {
        return this.username;
    }

    public List<String> getPermissionString() {
        return this.permissionList;
    }

    /**
     * An output stream for writing the information to another node
     * @param out The byte array output stream
     * @throws IOException Throw on failure
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(this.successful);
        out.writeString(this.username);
        out.writeStringArray((String[]) this.permissionList.toArray());
    }

    /**
     * Create a new PARSER that writes the different output fields
     */
    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetPermissionResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "get_permission_response_info",
        true,
        args -> new GetPermissionResponseInfo((boolean) args[0], (String) args[1], (List<String>) args[2])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareString(constructorArg(), new ParseField("permission list"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField USERNAME = new ParseField("username");
    private static final ParseField PERMISSION_LIST = new ParseField("permission list");

    /**
     * Write the response info to Xcontent (JSON formatted data) that you will see as the response message to the request
     * @param builder Xcontext instance
     * @param params Xcontent options
     * @return The modified instance which now contains the information from this response info object
     * @throws IOException throw on failure
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), this.successful);
        builder.field(USERNAME.getPreferredName(), this.username);
        builder.field(PERMISSION_LIST.getPreferredName(), this.permissionList);
        builder.endObject();
        return builder;
    }
}
