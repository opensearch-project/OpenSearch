/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rule.autotagging.Rule;

import java.io.IOException;

/**
 * Response for the update API for Rule
 * Example response:
 * {
 *     id": "z1MJApUB0zgMcDmz-UQq",
 *     "description": "Rule for tagging workload_group_id to index123"
 *     "index_pattern": ["index123"],
 *     "workload_group": "workload_group_id",
 *     "updated_at": "2025-02-14T01:19:22.589Z"
 * }
 * @opensearch.experimental
 */
@ExperimentalApi
public class UpdateRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final Rule rule;

    /**
     * constructor for UpdateRuleResponse
     * @param rule - the updated rule
     */
    public UpdateRuleResponse(final Rule rule) {
        this.rule = rule;
    }

    /**
     * Constructs a UpdateRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public UpdateRuleResponse(StreamInput in) throws IOException {
        this(new Rule(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        rule.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return rule.toXContent(builder, params);
    }

    /**
     * rule getter
     */
    public Rule getRule() {
        return rule;
    }
}
