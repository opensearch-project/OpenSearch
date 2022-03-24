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

public class PITResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("id");

    public String getId() {
        return id;
    }

    private final String id;

    PITResponse(String id) {
        this.id = id;
    }

    public PITResponse(StreamInput streamInput) throws IOException {
        id = streamInput.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);

    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.endObject();
        return builder;
    }
}
