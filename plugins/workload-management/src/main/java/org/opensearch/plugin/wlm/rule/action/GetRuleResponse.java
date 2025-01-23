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
import org.opensearch.wlm.Rule;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.wlm.Rule._ID_STRING;

/**
 * Response for the get API for Rule
 * @opensearch.experimental
 */
public class GetRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final Map<String, Rule> rules;
    private final RestStatus restStatus;

    /**
     * Constructor for GetRuleResponse
     * @param rules - The Map of Rules to be included in the response
     * @param restStatus - The restStatus for the response
     */
    public GetRuleResponse(final Map<String, Rule> rules, RestStatus restStatus) {
        this.rules = rules;
        this.restStatus = restStatus;
    }

    /**
     * Constructor for GetRuleResponse
     * @param in - A {@link StreamInput} object
     */
    public GetRuleResponse(StreamInput in) throws IOException {
        this.rules = in.readMap(StreamInput::readString, Rule::new);
        this.restStatus = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(rules, StreamOutput::writeString, (outStream, rule) -> rule.writeTo(outStream));
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
