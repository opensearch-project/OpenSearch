/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;

/**
 * Interface for a service that handles rule persistence CRUD operations.
 * @opensearch.experimental
 */
public interface RulePersistenceService {

    /**
     * Create rules based on the provided request.
     * @param request The request containing the details for creating the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener);

    /**
     * Get rules based on the provided request.
     * @param request The request containing the details for retrieving the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void getRule(GetRuleRequest request, ActionListener<GetRuleResponse> listener);

    /**
     * Delete a rule based on the provided request.
     * @param request The request containing the ID of the rule to delete.
     * @param listener The listener that will handle the response or failure.
     */
    void deleteRule(DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener);
}
