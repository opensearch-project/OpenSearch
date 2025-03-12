/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;

/**
 * Interface for a service that handles rule persistence CRUD operations.
 * @opensearch.experimental
 */
public interface RulePersistenceService {
    /**
     * Update rule based on the provided request.
     * @param request The request containing the details for updating the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener);
}
