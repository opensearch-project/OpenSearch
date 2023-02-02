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
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Response object holds multiple response info as a list
 * Responses from multiple actions are aggregated and then returned in this class
 */
public class PutPermissionResponse extends ActionResponse implements StatusToXContentObject {

    private final List<PutPermissionResponseInfo> putPermissionResults;

    public PutPermissionResponse(List<PutPermissionResponseInfo> putPermissionResults) {
        this.putPermissionResults = putPermissionResults;
    }

    public PutPermissionResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        putPermissionResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            putPermissionResults.add(new PutPermissionResponseInfo(in));
        }

    }

    public List<PutPermissionResponseInfo> getputPermissionResults() {
        return putPermissionResults;
    }

    /**
     * @return Whether the attempt to add a permission was successful
     */
    @Override
    public RestStatus status() {
        if (putPermissionResults.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(putPermissionResults.size());
        for (PutPermissionResponseInfo putPermissionResult : putPermissionResults) {
            putPermissionResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("permissions");
        for (PutPermissionResponseInfo response : putPermissionResults) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<PutPermissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "put_permission_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<PutPermissionResponseInfo> putPermissionResponseInfoList = (List<PutPermissionResponseInfo>) parsedObjects[0];
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
