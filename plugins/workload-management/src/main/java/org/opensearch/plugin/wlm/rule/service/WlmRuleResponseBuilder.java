/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.autotagging.Rule;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.rule.action.GetWlmRuleResponse;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.service.RuleResponseBuilder;

import java.util.Map;

@SuppressWarnings("unchecked")
public class WlmRuleResponseBuilder implements RuleResponseBuilder {
    @Override
    public <T extends GetRuleResponse> T buildGetRuleResponse(Map<String, Rule> ruleMap, String nextSearchAfter, RestStatus restStatus) {
        return (T) new GetWlmRuleResponse(ruleMap, nextSearchAfter, restStatus);
    }
}
