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
 * This class represents a delete rule event which can be consumed by {@link org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism}
 */
public class DeleteRuleEvent implements RuleEvent {
    private Rule deletedRule;
    private final InMemoryRuleProcessingService ruleProcessingService;

    /**
     * Constructor
     * @param deletedRule
     * @param ruleProcessingService
     */
    public DeleteRuleEvent(Rule deletedRule, InMemoryRuleProcessingService ruleProcessingService) {
        this.deletedRule = deletedRule;
        this.ruleProcessingService = ruleProcessingService;
    }

    @Override
    public void process() {
        ruleProcessingService.remove(deletedRule);
    }
}
