/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class captures if deletion of pit is successful along with pit id
 */
public class DeletePitInfo extends TransportResponse implements Writeable, ToXContent {
    /**
     * This will be true if PIT reader contexts are deleted ond also if contexts are not found.
     */
    private final boolean successful;

    private final String pitId;

    public DeletePitInfo(boolean successful, String pitId) {
        this.successful = successful;
        this.pitId = pitId;
    }

    public DeletePitInfo(StreamInput in) throws IOException {
        successful = in.readBoolean();
        pitId = in.readString();

    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getPitId() {
        return pitId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeString(pitId);
    }

    static final ConstructingObjectParser<DeletePitInfo, Void> PARSER = new ConstructingObjectParser<>(
        "delete_pit_info",
        true,
        args -> new DeletePitInfo((boolean) args[0], (String) args[1])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("pit_id"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField PIT_ID = new ParseField("pit_id");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), successful);
        builder.field(PIT_ID.getPreferredName(), pitId);
        builder.endObject();
        return builder;
    }

}
