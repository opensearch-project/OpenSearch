/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.wlm.Rule;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Response for the create API for Rule
 * @opensearch.experimental
 */
public class CreateRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final String _id;
    private final Rule rule;
    private final RestStatus restStatus;

    /**
     * Constructor for CreateRuleResponse
     * @param rule - The Rule to be included in the response
     * @param restStatus - The restStatus for the response
     */
    public CreateRuleResponse(String id, final Rule rule, RestStatus restStatus) {
        this._id = id;
        this.rule = rule;
        this.restStatus = restStatus;
    }

    /**
     * Constructor for CreateRuleResponse
     * @param in - A {@link StreamInput} object
     */
    public CreateRuleResponse(StreamInput in) throws IOException {
        _id = in.readString();
        rule = new Rule(in);
        restStatus = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(_id);
        rule.writeTo(out);
        RestStatus.writeTo(out, restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return rule.toXContent(builder, new MapParams(Map.of("_id", _id)));
    }

    /**
     * rule getter
     */
    public Rule getRule() {
        return rule;
    }

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }
}
