/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.core.action.ActionListener;

/**
 * Interface that handles rule routing logic
 * @opensearch.experimental
 */
public interface RuleRoutingService {

    /**
     * Handles a create rule request by routing it to the appropriate node.
     * @param request the create rule request
     * @param listener listener to handle the final response
     */
    void handleCreateRuleRequest(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener);
}
