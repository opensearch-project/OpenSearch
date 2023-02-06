/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.put;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.ParseField;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;


import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Response object holds multiple response info as a list
 * Responses from multiple actions are aggregated and then returned in this class
 */
public class PutPermissionResponse extends ActionResponse implements StatusToXContentObject {

    private final PutPermissionResponseInfo putPermissionResponseInfo;

    public PutPermissionResponse(PutPermissionResponseInfo permissionResponseInfo) {
        this.putPermissionResponseInfo = permissionResponseInfo;
    }

    /**
     * Conjoins the different PutPermissionResponseInfo objects from an input stream connected to another node
     * This is important for bulk requests but right now will just be a step in between the Info and returning to the client
     * @param in An input byte array stream from another node
     * @throws IOException Throw on failure
     */
    public PutPermissionResponse(StreamInput in) throws IOException {
        super(in);
        this.putPermissionResponseInfo = new PutPermissionResponseInfo(in);
    }

    public PutPermissionResponseInfo getPutPermissionResults() {
        return this.putPermissionResponseInfo;
    }

    /**
     * @return Whether the attempt to add a permission was successful
     */
    @Override
    public RestStatus status() {
        if (this.putPermissionResponseInfo == null) return NOT_FOUND;
        return OK;
    }

    /**
     * Sends the info out to another node
     * @param out An output stream to another node
     * @throws IOException Throw on failure
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (putPermissionResponseInfo != null) {
            putPermissionResponseInfo.writeTo(out);
        }
    }

    /**
     * Creates a builder for conjoining multiple response info objects
     * @param builder The Xcontent builder object that serves as the response holder
     * @param params Settings for the builder
     * @return A builder with all the concatenated response information
     * @throws IOException throw on failure
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (this.putPermissionResponseInfo != null) {
            putPermissionResponseInfo.toXContent(builder, params);
        }
        return builder;
    }

    /**
     * Conjoins the different PutPermissionResponseInfo but does so a different way
     */
    private static final ConstructingObjectParser<PutPermissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "put_permission_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            PutPermissionResponseInfo putPermissionResponseInfoList = (PutPermissionResponseInfo) parsedObjects[0];
            return new PutPermissionResponse(putPermissionResponseInfoList);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), PutPermissionResponseInfo.PARSER, new ParseField("permissions"));
    }

    public static PutPermissionResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
