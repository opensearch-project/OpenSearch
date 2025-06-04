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
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rule.autotagging.Rule;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.rule.autotagging.Rule._ID_STRING;

/**
 * Response for the create API for Rule
 * Example response:
 * {
 *    "_id":"wi6VApYBoX5wstmtU_8l",
 *    "description":"description1",
 *    "index_pattern":["log*", "uvent*"],
 *    "workload_group":"poOiU851RwyLYvV5lbvv5w",
 *    "updated_at":"2025-04-04T20:54:22.406Z"
 * }
 * @opensearch.experimental
 */
public class CreateRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final String _id;
    private final Rule rule;

    /**
     * contructor for CreateRuleResponse
     * @param id - the id for the rule created
     * @param rule - the rule created
     */
    public CreateRuleResponse(String id, final Rule rule) {
        this._id = id;
        this.rule = rule;
    }

    /**
     * Constructs a CreateRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public CreateRuleResponse(StreamInput in) throws IOException {
        _id = in.readString();
        rule = new Rule(in);
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
     * rule getter
     */
    public Rule getRule() {
        return rule;
    }
}
