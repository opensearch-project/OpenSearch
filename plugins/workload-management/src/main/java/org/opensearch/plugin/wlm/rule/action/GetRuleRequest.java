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
import org.opensearch.cluster.metadata.Rule;
import org.opensearch.cluster.metadata.Rule.Builder;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A request for get Rule
 * @opensearch.experimental
 */
public class GetRuleRequest extends ClusterManagerNodeRequest<GetRuleRequest> {
    private final Rule rule;

    /**
     * Constructor for GetRuleRequest
     * @param rule - A {@link Rule} object
     */
    GetRuleRequest(Rule rule) {
        this.rule = rule;
    }

    /**
     * Constructor for GetRuleRequest
     * @param in - A {@link StreamInput} object
     */
    GetRuleRequest(StreamInput in) throws IOException {
        super(in);
        rule = new Rule(in);
    }

    /**
     * Generate a GetRuleRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static GetRuleRequest fromXContent(XContentParser parser) throws IOException {
        Builder builder = Builder.fromXContent(parser);
        return new GetRuleRequest(builder._id(UUIDs.randomBase64UUID()).updatedAt(String.valueOf(Instant.now().getMillis())).build());
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
