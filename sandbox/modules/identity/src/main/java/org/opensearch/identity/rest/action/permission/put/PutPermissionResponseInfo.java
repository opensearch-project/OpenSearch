/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.put;

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
 * Response info corresponds to a single response for a permission put action
 */
public class PutPermissionResponseInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String username;
    private final String permissionString;


    /**
     * Construct an instance of a put permission response info (these are aggregated into a response for bulk requests)
     * @param successful whether the request was successful
     * @param username the username of the principal being granted the permission
     * @param permissionString the permission string to be granted
     */
    public PutPermissionResponseInfo(boolean successful, String username, String permissionString) {
        this.successful = successful;
        this.username = username;
        this.permissionString = permissionString;
    }

    /**
     * A stream based constructor
     * @param in an input stream for dealing with input from another node
     * @throws IOException Throw on failure
     */
    public PutPermissionResponseInfo(StreamInput in) throws IOException {
        this.successful = in.readBoolean();
        this.username = in.readString();
        this.permissionString = in.readString();

    }

    public boolean isSuccessful(){
        return this.successful;
    }
    public String getUsername() {
        return this.username;
    }
    public String getPermissionString() {
        return permissionString;
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
        out.writeString(this.permissionString);
    }

    /**
     * Create a new PARSER that writes the different output fields
     */
    static final ConstructingObjectParser<PutPermissionResponseInfo, Void> PARSER = new ConstructingObjectParser<>(
        "put_permission_response_info",
        true,
        args -> new PutPermissionResponseInfo((boolean) args[0], (String) args[1], (String) args[2])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareString(constructorArg(), new ParseField("permissionString"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField USERNAME = new ParseField("username");
    private static final ParseField PERMISSION_STRING = new ParseField("permission added");


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
        builder.field(PERMISSION_STRING.getPreferredName(), this.permissionString);
        builder.endObject();
        return builder;
    }
}
