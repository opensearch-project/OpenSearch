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
 * Response for the update API for Rule
 * @opensearch.experimental
 */
public class UpdateRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final String _id;
    private final Rule rule;
    private final RestStatus restStatus;

    /**
     * constructor for UpdateRuleResponse
     * @param _id - rule id updated
     * @param rule - the updated rule
     * @param restStatus - the status of UpdateRuleResponse
     */
    public UpdateRuleResponse(String _id, final Rule rule, RestStatus restStatus) {
        this._id = _id;
        this.rule = rule;
        this.restStatus = restStatus;
    }

    /**
     * Constructs a UpdateRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public UpdateRuleResponse(StreamInput in) throws IOException {
        this(in.readString(), new Rule(in), RestStatus.readFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(_id);
        rule.writeTo(out);
        RestStatus.writeTo(out, restStatus);
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

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }
}
