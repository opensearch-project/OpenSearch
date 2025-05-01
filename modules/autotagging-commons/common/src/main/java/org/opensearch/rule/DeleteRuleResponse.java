/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for delete Rule API
 * Example response:
 * {
 *   "acknowledged": true
 * }
 * @opensearch.experimental
 */
public class DeleteRuleResponse extends ActionResponse implements ToXContentObject {
    private final boolean acknowledged;

    /**
     * Constructor
     * @param acknowledged whether the delete was acknowledged
     */
    public DeleteRuleResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Deserialization constructor
     * @param in Stream input
     */
    public DeleteRuleResponse(StreamInput in) throws IOException {
        this.acknowledged = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", acknowledged);
        builder.endObject();
        return builder;
    }

    /**
     * Returns whether the rule deletion was acknowledged.
     *
     * @return true if the rule was successfully deleted; false otherwise.
     */
    public boolean isAcknowledged() {
        return acknowledged;
    }
}
