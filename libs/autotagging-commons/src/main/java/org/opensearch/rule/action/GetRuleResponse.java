/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.autotagging.Rule;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * Response for the get API for Rule.
 * Example response:
 * {
 *     "rules": [
 *         {
 *             "_id": "z1MJApUB0zgMcDmz-UQq",
 *             "description": "Rule for tagging query_group_id to index123"
 *             "index_pattern": ["index123"],
 *             "query_group": "query_group_id",
 *             "updated_at": "2025-02-14T01:19:22.589Z"
 *         },
 *         ...
 *     ],
 *     "search_after": ["z1MJApUB0zgMcDmz-UQq"]
 * }
 * @opensearch.experimental
 */
public class GetRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final Map<String, Rule> rules;
    private final String searchAfter;
    private final RestStatus restStatus;

    /**
     * Constructor for GetRuleResponse
     * @param rules - Rules get from the request
     * @param searchAfter - The sort value used for pagination.
     * @param restStatus - Status of the GetRuleResponse
     */
    public GetRuleResponse(final Map<String, Rule> rules, String searchAfter, RestStatus restStatus) {
        this.rules = rules;
        this.searchAfter = searchAfter;
        this.restStatus = restStatus;
    }

    /**
     * Constructs a GetRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public GetRuleResponse(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, Rule::new), in.readOptionalString(), RestStatus.readFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(rules, StreamOutput::writeString, (outStream, rule) -> rule.writeTo(outStream));
        out.writeOptionalString(searchAfter);
        RestStatus.writeTo(out, restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("rules");
        for (Map.Entry<String, Rule> entry : rules.entrySet()) {
            entry.getValue().toXContent(builder, new MapParams(Map.of(_ID_STRING, entry.getKey())));
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
    public Map<String, Rule> getRules() {
        return rules;
    }

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }
}
