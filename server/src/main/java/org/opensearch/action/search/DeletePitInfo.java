/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

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
 * This class captures if deletion of pit is successful along with pit id
 */
public class DeletePitInfo extends TransportResponse implements Writeable, ToXContent {
    /**
     * This will be true if PIT reader contexts are deleted ond also if contexts are not found.
     */
    private final boolean succeeded;

    private final String pitId;

    public DeletePitInfo(boolean succeeded, String pitId) {
        this.succeeded = succeeded;
        this.pitId = pitId;
    }

    public DeletePitInfo(StreamInput in) throws IOException {
        succeeded = in.readBoolean();
        pitId = in.readString();

    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public String getPitId() {
        return pitId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(succeeded);
        out.writeString(pitId);
    }

    static final ConstructingObjectParser<DeletePitInfo, Void> PARSER = new ConstructingObjectParser<>(
        "delete_pit_info",
        true,
        args -> new DeletePitInfo((boolean) args[0], (String) args[1])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("succeeded"));
        PARSER.declareString(constructorArg(), new ParseField("pitId"));
    }

    private static final ParseField SUCCEEDED = new ParseField("succeeded");
    private static final ParseField PIT_ID = new ParseField("pitId");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCEEDED.getPreferredName(), succeeded);
        builder.field(PIT_ID.getPreferredName(), pitId);
        builder.endObject();
        return builder;
    }

}
