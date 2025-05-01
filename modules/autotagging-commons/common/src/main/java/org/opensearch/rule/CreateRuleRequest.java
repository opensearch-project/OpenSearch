/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rule.autotagging.FeatureValueValidator;
import org.opensearch.rule.autotagging.Rule;

import java.io.IOException;

/**
 * A request for create Rule
 * Example request:
 * Note that the endpoint below is for wlm rules specifically and serves only as an example
 * curl -XPUT "localhost:9200/_wlm/rule/" -H 'Content-Type: application/json' -d '
 * {
 *      "description": "description1",
 *      "index_pattern": ["log*", "event*"],
 *      "workload_group": "poOiU851RwyLYvV5lbvv5w"
 * }'
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
        try {
            FeatureValueValidator featureValueValidator = rule.getFeatureType().getFeatureValueValidator();
            if (featureValueValidator == null) {
                throw new IllegalStateException("FeatureValueValidator is not defined for feature type " + rule.getFeatureType().getName());
            }
            featureValueValidator.validate(rule.getFeatureValue());
            return null;
        } catch (Exception e) {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            validationException.addValidationError("Validation failed: " + e.getMessage());
            return validationException;
        }
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
