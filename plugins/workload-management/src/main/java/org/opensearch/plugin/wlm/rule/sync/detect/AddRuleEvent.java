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
 * This class represents an add rule event which can be consumed by {@link org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism}
 */
public class AddRuleEvent implements RuleEvent {
    private final Rule newRule;
    private final InMemoryRuleProcessingService ruleProcessingService;

    /**
     * Constructor
     * @param newRule
     * @param ruleProcessingService
     */
    public AddRuleEvent(Rule newRule, InMemoryRuleProcessingService ruleProcessingService) {
        this.newRule = newRule;
        this.ruleProcessingService = ruleProcessingService;
    }

    @Override
    public void process() {
        ruleProcessingService.add(newRule);
    }
}
