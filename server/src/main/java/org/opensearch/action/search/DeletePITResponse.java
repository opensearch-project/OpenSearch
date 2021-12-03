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
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

public class DeletePITResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");

    private final boolean acknowledged;

    public DeletePITResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public DeletePITResponse(StreamInput streamInput) throws IOException {
        acknowledged = streamInput.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;//TODO
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED.getPreferredName(), acknowledged);
        builder.endObject();
        return builder;
    }
}
