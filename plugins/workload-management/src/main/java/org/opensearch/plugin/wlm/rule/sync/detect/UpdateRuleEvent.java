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

    /**
     * Constructor
     * @param previousRule
     * @param newRule
     */
    public UpdateRuleEvent(Rule previousRule, Rule newRule) {
        this.previousRule = previousRule;
        this.newRule = newRule;
    }

    @Override
    public void process(InMemoryRuleProcessingService inMemoryRuleProcessingService) {
        inMemoryRuleProcessingService.remove(previousRule);
        inMemoryRuleProcessingService.add(newRule);
    }
}
