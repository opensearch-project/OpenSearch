/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for deleting a Rule.
 * @opensearch.experimental
 */
public class DeleteRuleResponse extends ActionResponse implements ToXContentObject {
    private final boolean acknowledged;
    private final RestStatus status;

    public DeleteRuleResponse(boolean acknowledged, RestStatus status) {
        this.acknowledged = acknowledged;
        this.status = status;
    }

    public DeleteRuleResponse(StreamInput in) throws IOException {
        this.acknowledged = in.readBoolean();
        this.status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        RestStatus.writeTo(out, status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("acknowledged", acknowledged)
            .endObject();
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public RestStatus getStatus() {
        return status;
    }
}

