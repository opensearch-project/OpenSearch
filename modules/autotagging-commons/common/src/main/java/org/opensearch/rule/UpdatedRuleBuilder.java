/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rule.autotagging.Rule;

/**
 * A functional interface for updating an existing {@link Rule} using an {@link UpdateRuleRequest}.
 * @opensearch.experimental
 */
@ExperimentalApi
public interface UpdatedRuleBuilder {
    /**
     * Applies updates to an existing rule based on the provided update request.
     * @param existingRule the rule to update
     * @param request the update request containing new values
     */
    Rule apply(Rule existingRule, UpdateRuleRequest request);
}
