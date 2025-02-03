/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.joda.time.Instant;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.wlm.Rule;
import org.opensearch.wlm.Rule.Builder;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.wlm.querygroup.action.UpdateQueryGroupRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A request for update Rule
 * @opensearch.experimental
 */
public class UpdateRuleRequest extends ClusterManagerNodeRequest<UpdateRuleRequest> {
//    private final String _id;
//    private final Map<Rule.RuleAttribute, List<String>> attributeMap;
//    private final String label;
    private Rule rule;

    /**
     * Constructor for UpdateRuleRequest
     * @param rule - A {@link Rule} object
     */
    UpdateRuleRequest(Rule rule) {
        this.rule = rule;
    }

    /**
     * Constructor for UpdateRuleRequest
     * @param in - A {@link StreamInput} object
     */
    UpdateRuleRequest(StreamInput in) throws IOException {
        super(in);
        rule = new Rule(in);
    }

    /**
     * Generate a UpdateRuleRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static UpdateRuleRequest fromXContent(XContentParser parser, String _id) throws IOException {
        Builder builder = Builder.fromXContent(parser);
        if (builder.getLabel() == null) {
            builder.label(""); //TODO: check
        }
        return new UpdateRuleRequest(builder.updatedAt(Instant.now().toString()).build());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        rule.writeTo(out);
    }

    /**
     * Rule getter
     */
    public Rule getRule() {
        return rule;
    }
}
