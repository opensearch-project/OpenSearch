/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.get;

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
public class GetPermissionResponse extends ActionResponse implements StatusToXContentObject {

    private final GetPermissionResponseInfo getPermissionResponseInfo;

    public GetPermissionResponse(GetPermissionResponseInfo permissionResponseInfo) {
        this.getPermissionResponseInfo = permissionResponseInfo;
    }

    /**
     * Conjoins the different GetPermissionResponseInfo objects from an inget stream connected to another node
     * This is important for bulk requests but right now will just be a step in between the Info and returning to the client
     * @param in An inget byte array stream from another node
     * @throws IOException Throw on failure
     */
    public GetPermissionResponse(StreamInput in) throws IOException {
        super(in);
        this.getPermissionResponseInfo = new GetPermissionResponseInfo(in);
    }

    public GetPermissionResponseInfo getGetPermissionResults() {
        return this.getPermissionResponseInfo;
    }

    /**
     * @return Whether the attempt to fetch the permissions was successful
     */
    @Override
    public RestStatus status() {
        if (this.getPermissionResponseInfo == null) return NOT_FOUND;
        return OK;
    }

    /**
     * Sends the info out to another node
     * @param out An outget stream to another node
     * @throws IOException Throw on failure
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (getPermissionResponseInfo != null) {
            getPermissionResponseInfo.writeTo(out);
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
        if (this.getPermissionResponseInfo != null) {
            getPermissionResponseInfo.toXContent(builder, params);
        }
        return builder;
    }

    /**
     * Conjoins the different GetPermissionResponseInfo but does so a different way
     */
    private static final ConstructingObjectParser<GetPermissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_permission_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            GetPermissionResponseInfo getPermissionResponseInfoList = (GetPermissionResponseInfo) parsedObjects[0];
            return new GetPermissionResponse(getPermissionResponseInfoList);
        }
    );
    static {
        PARSER.declareObject(constructorArg(), GetPermissionResponseInfo.PARSER, new ParseField("permissions"));
    }

    public static GetPermissionResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
