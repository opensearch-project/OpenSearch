/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ObjectParser;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

/**
 * Response class for delete pit flow which returns if the contexts are freed
 */
public class DeletePitResponse extends ActionResponse implements StatusToXContentObject {

    /**
     * This will be true if all PIT reader contexts are deleted.
     */
    private final boolean succeeded;

    public DeletePitResponse(boolean succeeded) {
        this.succeeded = succeeded;
    }

    public DeletePitResponse(StreamInput in) throws IOException {
        super(in);
        succeeded = in.readBoolean();
    }

    /**
     * @return Whether the attempt to delete PIT was successful.
     */
    public boolean isSucceeded() {
        return succeeded;
    }

    @Override
    public RestStatus status() {
        return succeeded ? OK : NOT_FOUND;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(succeeded);
    }

    private static final ParseField SUCCEEDED = new ParseField("succeeded");

    private static final ConstructingObjectParser<DeletePitResponse, Void> PARSER = new ConstructingObjectParser<>(
        "delete_pit",
        true,
        a -> new DeletePitResponse((boolean) a[0])
    );
    static {
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), SUCCEEDED, ObjectParser.ValueType.BOOLEAN);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCEEDED.getPreferredName(), succeeded);
        builder.endObject();
        return builder;
    }

    /**
     * Parse the delete PIT response body into a new {@link DeletePitResponse} object
     */
    public static DeletePitResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

}
