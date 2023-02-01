/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.add;

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

public class AddPermissionResponse extends ActionResponse implements StatusToXContentObject {

    // TODO: revisit this class
    private final List<AddPermissionResponseInfo> addPermissionResults;

    public AddPermissionResponse(List<AddPermissionResponseInfo> addPermissionResults) {
        this.addPermissionResults = addPermissionResults;
    }

    public AddPermissionResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        addPermissionResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            addPermissionResults.add(new AddPermissionResponseInfo(in));
        }

    }

    public List<AddPermissionResponseInfo> getAddPermissionResults() {
        return addPermissionResults;
    }

    /**
     * @return Whether the attempt to add a permission was successful
     */
    @Override
    public RestStatus status() {
        if (addPermissionResults.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(addPermissionResults.size());
        for (AddPermissionResponseInfo addPermissionResult : addPermissionResults) {
            addPermissionResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("permissions");
        for (AddPermissionResponseInfo response : addPermissionResults) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<AddPermissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "add_permission_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<AddPermissionResponseInfo> addPermissionResponseInfoList = (List<AddPermissionResponseInfo>) parsedObjects[0];
            return new AddPermissionResponse(addPermissionResponseInfoList);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), AddPermissionResponseInfo.PARSER, new ParseField("permissions"));
    }

    public static AddPermissionResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
