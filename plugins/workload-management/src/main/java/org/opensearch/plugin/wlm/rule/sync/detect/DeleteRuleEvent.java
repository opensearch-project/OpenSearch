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

    /**
     * Constructor
     * @param deletedRule
     */
    public DeleteRuleEvent(Rule deletedRule) {
        this.deletedRule = deletedRule;
    }

    @Override
    public void process(InMemoryRuleProcessingService inMemoryRuleProcessingService) {
        inMemoryRuleProcessingService.remove(deletedRule);
    }
}
