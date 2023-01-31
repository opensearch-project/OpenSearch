/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.response;

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


public class DeletePermissionResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final List<DeletePermissionResponseInfo> deletePermissionResults;

    public DeletePermissionResponse(List<DeletePermissionResponseInfo> DeletePermissionResults) {
        this.deletePermissionResults = DeletePermissionResults;
    }

    public DeletePermissionResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        deletePermissionResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            deletePermissionResults.add(new DeletePermissionResponseInfo(in));
        }

    }

    public List<DeletePermissionResponseInfo> getDeletePermissionResults() {
        return deletePermissionResults;
    }

    /**
     * @return Whether the attempt to delete a permission was successful
     */
    @Override
    public RestStatus status() {
        if (deletePermissionResults.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(deletePermissionResults.size());
        for (DeletePermissionResponseInfo deletePermissionResult : deletePermissionResults) {
            deletePermissionResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("permissions");
        for (DeletePermissionResponseInfo response : deletePermissionResults) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<DeletePermissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "delete_permission_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<DeletePermissionResponseInfo> deletePermissionResponseInfoList = (List<DeletePermissionResponseInfo>) parsedObjects[0];
            return new DeletePermissionResponse(deletePermissionResponseInfoList);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), DeletePermissionResponseInfo.PARSER, new ParseField("permissions"));
    }

    public static DeletePermissionResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}


