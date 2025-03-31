/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.autotagging.Rule;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rule.action.GetRuleResponse;

import java.io.IOException;
import java.util.Map;

/**
 * A response to get workload management Rules in workload management
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
public class GetWlmRuleResponse extends GetRuleResponse {
    /**
     * Constructor for GetWlmRuleResponse
     * @param rules - A map of rule IDs to objects representing the retrieved rules
     * @param searchAfter - A string used for pagination
     * @param restStatus - The {@link RestStatus} indicating the status of the request.
     */
    public GetWlmRuleResponse(Map<String, Rule> rules, String searchAfter, RestStatus restStatus) {
        super(rules, searchAfter, restStatus);
    }

    /**
     * Constructs a new GetWlmRuleResponse instance from StreamInput
     * @param in The {@link StreamInput} from which to read the response data.
     */
    public GetWlmRuleResponse(StreamInput in) throws IOException {
        super(in);
    }
}
