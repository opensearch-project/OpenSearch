/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.sync.detect;

import org.opensearch.plugin.wlm.AutoTaggingActionFilterTests;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class RuleEventClassifierTests extends OpenSearchTestCase {
    public static final int TOTAL_RULE_COUNT = 10;
    RuleEventClassifier sut;

    InMemoryRuleProcessingService ruleProcessingService = mock(InMemoryRuleProcessingService.class);

    public void testGetRuleEventsForAddRuleEvents() {
        sut = new RuleEventClassifier(Collections.emptySet(), ruleProcessingService);
        Set<Rule> newRules = new HashSet<>();
        for (int i = 0; i < TOTAL_RULE_COUNT; i++) {
            newRules.add(getRandomRule(String.valueOf(i)));
        }

        List<RuleEvent> addRuleEvents = sut.getRuleEvents(newRules);
        assertEquals(TOTAL_RULE_COUNT, addRuleEvents.size());
        assertTrue(addRuleEvents.stream().allMatch(ruleEvent -> ruleEvent instanceof AddRuleEvent));
    }

    public void testDeleteRuleEvents() {
        Set<Rule> previousRules = new HashSet<>();
        for (int i = 0; i < TOTAL_RULE_COUNT; i++) {
            previousRules.add(getRandomRule(String.valueOf(i)));
        }
        final int EXISTING_RULE_COUNT = 3;
        Set<Rule> newRules = new HashSet<>();
        Iterator<Rule> ruleIterator = previousRules.iterator();
        for (int i = 0; i < EXISTING_RULE_COUNT; i++) {
            newRules.add(ruleIterator.next());
        }
        sut = new RuleEventClassifier(previousRules, ruleProcessingService);
        List<RuleEvent> deleteRuleEvents = sut.getRuleEvents(newRules);

        assertEquals(TOTAL_RULE_COUNT - EXISTING_RULE_COUNT, deleteRuleEvents.size());
        assertTrue(deleteRuleEvents.stream().allMatch(ruleEvent -> ruleEvent instanceof DeleteRuleEvent));

    }

    public void testUpdateRuleEvents() {
        Set<Rule> previousRules = new HashSet<>();
        Set<Rule> newRules = new HashSet<>();
        final int EXISTING_RULE_COUNT = 3;
        for (int i = 0; i < TOTAL_RULE_COUNT; i++) {
            Rule randomRule = getRandomRule(String.valueOf(i));
            previousRules.add(randomRule);
            if (i < EXISTING_RULE_COUNT) {
                newRules.add(randomRule);
            }
        }

        for (int i = EXISTING_RULE_COUNT; i < TOTAL_RULE_COUNT; i++) {
            newRules.add(getRandomRule(String.valueOf(i)));
        }
        sut = new RuleEventClassifier(previousRules, ruleProcessingService);
        List<RuleEvent> deleteRuleEvents = sut.getRuleEvents(newRules);

        assertEquals(TOTAL_RULE_COUNT - EXISTING_RULE_COUNT, deleteRuleEvents.size());
        assertTrue(deleteRuleEvents.stream().allMatch(ruleEvent -> ruleEvent instanceof UpdateRuleEvent));

    }

    public static Rule getRandomRule(String id) {
        return Rule.builder()
            .id(id)
            .description("description")
            .attributeMap(Map.of(AutoTaggingActionFilterTests.TestAttribute.TEST_ATTRIBUTE, Set.of(randomAlphaOfLengthBetween(2, 10))))
            .featureValue(randomAlphaOfLengthBetween(2, 10))
            .updatedAt("2025-05-27T08:58:57.558Z")
            .featureType(AutoTaggingActionFilterTests.WLMFeatureType.WLM)
            .build();
    }
}
