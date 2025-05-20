/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rule.autotagging.Rule;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.rule.autotagging.Rule._ID_STRING;

/**
 * Response for the update API for Rule
 * Example response:
 * {
 *     _id": "z1MJApUB0zgMcDmz-UQq",
 *     "description": "Rule for tagging workload_group_id to index123"
 *     "index_pattern": ["index123"],
 *     "workload_group": "workload_group_id",
 *     "updated_at": "2025-02-14T01:19:22.589Z"
 * }
 * @opensearch.experimental
 */
@ExperimentalApi
public class UpdateRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final String _id;
    private final Rule rule;

    /**
     * constructor for UpdateRuleResponse
     * @param _id - rule id updated
     * @param rule - the updated rule
     */
    public UpdateRuleResponse(String _id, final Rule rule) {
        this._id = _id;
        this.rule = rule;
    }

    /**
     * Constructs a UpdateRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public UpdateRuleResponse(StreamInput in) throws IOException {
        this(in.readString(), new Rule(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(_id);
        rule.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return rule.toXContent(builder, new MapParams(Map.of(_ID_STRING, _id)));
    }

    /**
     * id getter
     */
    public String get_id() {
        return _id;
    }

    /**
     * rule getter
     */
    public Rule getRule() {
        return rule;
    }
}
