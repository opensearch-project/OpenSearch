/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.check;

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

public class CheckPermissionResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final List<CheckPermissionResponseInfo> checkPermissionResults;

    public CheckPermissionResponse(List<CheckPermissionResponseInfo> CheckPermissionResults) {
        this.checkPermissionResults = CheckPermissionResults;
    }

    public CheckPermissionResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        checkPermissionResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            checkPermissionResults.add(new CheckPermissionResponseInfo(in));
        }
    }

    public List<CheckPermissionResponseInfo> getCheckPermissionResults() {
        return checkPermissionResults;
    }

    /**
     * @return Whether the attempt to check a permission was successful
     */
    @Override
    public RestStatus status() {
        if (checkPermissionResults.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(checkPermissionResults.size());
        for (CheckPermissionResponseInfo checkPermissionResult : checkPermissionResults) {
            checkPermissionResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("permissions");
        for (CheckPermissionResponseInfo response : checkPermissionResults) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<CheckPermissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "check_permission_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<CheckPermissionResponseInfo> checkPermissionResponseInfoList = (List<CheckPermissionResponseInfo>) parsedObjects[0];
            return new CheckPermissionResponse(checkPermissionResponseInfoList);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), CheckPermissionResponseInfo.PARSER, new ParseField("permissions"));
    }

    public static CheckPermissionResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
