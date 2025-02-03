/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.wlm.Rule;
import org.opensearch.wlm.Rule.Builder;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * A request for create Rule
 * @opensearch.experimental
 */
public class CreateRuleRequest extends ClusterManagerNodeRequest<CreateRuleRequest> {
    private final Rule rule;

    /**
     * Constructor for CreateRuleRequest
     * @param rule - A {@link Rule} object
     */
    CreateRuleRequest(Rule rule) {
        this.rule = rule;
    }

    /**
     * Constructor for CreateRuleRequest
     * @param in - A {@link StreamInput} object
     */
    CreateRuleRequest(StreamInput in) throws IOException {
        super(in);
        rule = new Rule(in);
    }

    /**
     * Generate a CreateRuleRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static CreateRuleRequest fromXContent(XContentParser parser) throws IOException {
        Builder builder = Builder.fromXContent(parser);
        return new CreateRuleRequest(builder.updatedAt(Instant.now().toString()).build());
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
