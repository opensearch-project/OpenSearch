/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.metadata.Rule;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for the get API for Rule
 *
 * @opensearch.experimental
 */
public class GetRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final List<Rule> rules;
    private final RestStatus restStatus;

    /**
     * Constructor for GetRuleResponse
     * @param rules - The List of Rules to be included in the response
     * @param restStatus - The restStatus for the response
     */
    public GetRuleResponse(final List<Rule> rules, RestStatus restStatus) {
        this.rules = rules;
        this.restStatus = restStatus;
    }

    /**
     * Constructor for GetRuleResponse
     * @param in - A {@link StreamInput} object
     */
    public GetRuleResponse(StreamInput in) throws IOException {
        this.rules = in.readList(Rule::new);
        restStatus = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(rules);
        RestStatus.writeTo(out, restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("rules");
        for (Rule rule : rules) {
            rule.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * rules getter
     */
    public List<Rule> getRules() {
        return rules;
    }

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }
}
