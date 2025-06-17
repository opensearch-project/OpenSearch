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
import java.util.List;

/**
 * Response for the get API for Rule.
 * Example response:
 * {
 *     "rules": [
 *         {
 *             "id": "z1MJApUB0zgMcDmz-UQq",
 *             "description": "Rule for tagging workload_group_id to index123"
 *             "index_pattern": ["index123"],
 *             "workload_group": "workload_group_id",
 *             "updated_at": "2025-02-14T01:19:22.589Z"
 *         },
 *         ...
 *     ],
 *     "search_after": ["z1MJApUB0zgMcDmz-UQq"]
 * }
 * @opensearch.experimental
 */
@ExperimentalApi
public class GetRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final List<Rule> rules;
    private final String searchAfter;

    /**
     * Constructor for GetRuleResponse
     * @param rules - Rules get from the request
     * @param searchAfter - The sort value used for pagination.
     */
    public GetRuleResponse(final List<Rule> rules, String searchAfter) {
        this.rules = rules;
        this.searchAfter = searchAfter;
    }

    /**
     * Constructs a GetRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public GetRuleResponse(StreamInput in) throws IOException {
        this(in.readList(Rule::new), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(rules);
        out.writeOptionalString(searchAfter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("rules");
        for (Rule rule : rules) {
            rule.toXContent(builder, params);
        }
        builder.endArray();
        if (searchAfter != null && !searchAfter.isEmpty()) {
            builder.field("search_after", new Object[] { searchAfter });
        }
        builder.endObject();
        return builder;
    }

    /**
     * rules getter
     */
    public List<Rule> getRules() {
        return rules;
    }
}
