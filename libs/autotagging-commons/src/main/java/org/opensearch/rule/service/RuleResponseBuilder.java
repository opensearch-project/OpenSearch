/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.opensearch.autotagging.Rule;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rule.action.GetRuleResponse;

import java.util.Map;

/**
 * A builder interface for rule lifecycle response objects.
 * @opensearch.experimental
 */
public interface RuleResponseBuilder {
    /**
     * Builds a GetRuleResponse object containing rule data.
     * @param ruleMap        A map of rule IDs to their corresponding {@link Rule} objects.
     * @param nextSearchAfter The ID used for pagination to fetch the next set of results, if applicable.
     * @param restStatus      The response rest status.
     */
    <T extends GetRuleResponse> T buildGetRuleResponse(Map<String, Rule> ruleMap, String nextSearchAfter, RestStatus restStatus);
}
