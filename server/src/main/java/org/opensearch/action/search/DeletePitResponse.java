/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionResponse;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Response class for delete pits flow which clears the point in time search contexts
 */
public class DeletePitResponse extends ActionResponse implements StatusToXContentObject {

    private final List<DeletePitInfo> deletePitResults;

    public DeletePitResponse(List<DeletePitInfo> deletePitResults) {
        this.deletePitResults = deletePitResults;
    }

    public DeletePitResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        deletePitResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            deletePitResults.add(new DeletePitInfo(in));
        }

    }

    public List<DeletePitInfo> getDeletePitResults() {
        return deletePitResults;
    }

    /**
     * @return Whether the attempt to delete PIT was successful.
     */
    @Override
    public RestStatus status() {
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(deletePitResults.size());
        for (DeletePitInfo deletePitResult : deletePitResults) {
            deletePitResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("pits");
        for (DeletePitInfo response : deletePitResults) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<DeletePitResponse, Void> PARSER = new ConstructingObjectParser<>(
        "delete_pit_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<DeletePitInfo> deletePitInfoList = (List<DeletePitInfo>) parsedObjects[0];
            return new DeletePitResponse(deletePitInfoList);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), DeletePitInfo.PARSER, new ParseField("pits"));
    }

    public static DeletePitResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
