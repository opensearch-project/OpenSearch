/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.autotagging.Rule;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request for create Rule
 * @opensearch.experimental
 */
public class CreateRuleRequest extends ActionRequest {
    private final Rule rule;

    /**
     * constructor for CreateRuleRequest
     * @param rule the rule to create
     */
    public CreateRuleRequest(Rule rule) {
        this.rule = rule;
    }

    /**
     * Constructs a CreateRuleRequest from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public CreateRuleRequest(StreamInput in) throws IOException {
        super(in);
        rule = new Rule(in);
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
     * rule getter
     */
    public Rule getRule() {
        return rule;
    }
}
