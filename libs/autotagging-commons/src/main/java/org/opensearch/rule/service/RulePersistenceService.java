/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;

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
}
