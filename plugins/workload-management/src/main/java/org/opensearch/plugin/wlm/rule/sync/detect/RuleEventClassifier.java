/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.sync.detect;

import org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.autotagging.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is responsible for determining new {@link RuleEvent}s based on the previous snapshot of {@link Rule}s
 * <b>Warning:</b>  This class is not thread-safe and is expected to be called from a single thread only
 */
public class RuleEventClassifier {
    private Set<Rule> previousRules;
    private Map<String, Rule> previousRuleMap;
    private Map<String, Rule> newRuleMap;
    private final InMemoryRuleProcessingService ruleProcessingService;

    /**
     * Constructor
     *
     * @param previousRules
     * @param ruleProcessingService
     */
    public RuleEventClassifier(Set<Rule> previousRules, InMemoryRuleProcessingService ruleProcessingService) {
        this.previousRules = previousRules;
        this.ruleProcessingService = ruleProcessingService;
        this.previousRuleMap = previousRules.stream().collect(Collectors.toMap(Rule::getId, rule -> rule));
    }

    /**
     * This method classifies the rules based on the previous and new rules
     * and returns the appropriate rule events.
     * @param newRules Set of new rules
     * @return List of rule events
     */
    public List<RuleEvent> getRuleEvents(Set<Rule> newRules) {
        this.newRuleMap = newRules.stream().collect(Collectors.toMap(Rule::getId, rule -> rule));
        List<RuleEvent> ruleEvents = new ArrayList<>();

        // handles updates and add
        for (Rule newRule : newRules) {
            if (isPotentialNewRule(newRule)) {
                if (isRuleUpdated(newRule)) {
                    ruleEvents.add(new UpdateRuleEvent(previousRuleMap.get(newRule.getId()), newRule, ruleProcessingService));
                } else {
                    ruleEvents.add(new AddRuleEvent(newRule, ruleProcessingService));
                }
            }
        }

        // handles deletes
        for (Rule previousRule : previousRules) {
            if (isRuleDeleted(previousRule)) {
                ruleEvents.add(new DeleteRuleEvent(previousRule, ruleProcessingService));
            }
        }
        return ruleEvents;
    }

    private boolean isPotentialNewRule(Rule rule) {
        return !previousRules.contains(rule);
    }

    private boolean isRuleUpdated(Rule rule) {
        return previousRuleMap.containsKey(rule.getId());
    }

    private boolean isRuleDeleted(Rule rule) {
        return !newRuleMap.containsKey(rule.getId());
    }

    /**
     * Sets the previous rules and updates the previous rule map
     * which will be used to compute rule change events during next {@link RefreshBasedSyncMechanism} run
     * @param previousRules
     */
    public void setPreviousRules(Set<Rule> previousRules) {
        this.previousRules = previousRules;
        this.previousRuleMap = previousRules.stream().collect(Collectors.toMap(Rule::getId, rule -> rule));
    }
}
