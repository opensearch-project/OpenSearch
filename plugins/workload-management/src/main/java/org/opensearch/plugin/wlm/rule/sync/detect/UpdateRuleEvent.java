/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.sync.detect;

import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.autotagging.Rule;

/**
 * This class represents an update rule event which can be consumed by {@link org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism}
 */
public class UpdateRuleEvent implements RuleEvent {
    private final Rule previousRule;
    private final Rule newRule;
    private final InMemoryRuleProcessingService ruleProcessingService;

    /**
     * Constructor
     * @param previousRule
     * @param newRule
     * @param ruleProcessingService
     */
    public UpdateRuleEvent(Rule previousRule, Rule newRule, InMemoryRuleProcessingService ruleProcessingService) {
        this.previousRule = previousRule;
        this.newRule = newRule;
        this.ruleProcessingService = ruleProcessingService;
    }

    @Override
    public void process() {
        ruleProcessingService.remove(previousRule);
        ruleProcessingService.add(newRule);
    }
}
