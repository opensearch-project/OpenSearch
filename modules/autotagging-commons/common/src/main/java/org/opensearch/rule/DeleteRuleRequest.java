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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to delete a Rule by ID
 * Example:
 * curl -XDELETE "localhost:9200/_rules/{featureType}/ruleId"
 * @opensearch.experimental
 */
@ExperimentalApi
public class DeleteRuleRequest extends ActionRequest {
    private final String ruleId;

    /**
     * Constructs a request to delete a rule.
     *
     * @param ruleId The ID of the rule to delete.
     */
    public DeleteRuleRequest(String ruleId) {
        this.ruleId = ruleId;
    }

    /**
     * Deserialization constructor
     * @param in Stream input
     */
    public DeleteRuleRequest(StreamInput in) throws IOException {
        super(in);
        this.ruleId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(ruleId);
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
}
