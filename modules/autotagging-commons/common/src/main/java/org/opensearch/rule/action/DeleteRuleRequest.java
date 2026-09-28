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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rule.autotagging.FeatureType;

import java.io.IOException;

/**
 * A request to delete a Rule by ID
 * Example:
 * curl -XDELETE "localhost:9200/_rules/{featureType}/{_id}"
 * @opensearch.experimental
 */
@ExperimentalApi
public class DeleteRuleRequest extends ActionRequest {
    private final String ruleId;
    private final FeatureType featureType;

    /**
     * Constructs a request to delete a rule.
     *
     * @param ruleId The ID of the rule to delete.
     * @param featureType The feature type associated with the rule.
     */
    public DeleteRuleRequest(String ruleId, FeatureType featureType) {
        this.ruleId = ruleId;
        this.featureType = featureType;
    }

    /**
     * Deserialization constructor
     * @param in Stream input
     */
    public DeleteRuleRequest(StreamInput in) throws IOException {
        super(in);
        this.ruleId = in.readString();
        this.featureType = FeatureType.from(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(ruleId);
        featureType.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (ruleId == null || ruleId.isEmpty()) {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            validationException.addValidationError("Rule ID is missing");
            return validationException;
        }
        return null;
    }

    /**
     * Returns the ID of the rule to be deleted.
     *
     * @return The rule ID.
     */
    public String getRuleId() {
        return ruleId;
    }

    /**
     * Returns the feature type associated with the rule.
     *
     * @return The feature type.
     */
    FeatureType getFeatureType() {
        return featureType;
    }
}
