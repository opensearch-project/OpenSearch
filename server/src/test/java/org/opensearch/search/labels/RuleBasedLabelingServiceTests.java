/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.labels.rules.DefaultUserInfoLabelingRule;
import org.opensearch.search.labels.rules.Rule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RuleBasedLabelingServiceTests extends OpenSearchTestCase {
    private RuleBasedLabelingService ruleBasedLabelingService;
    private ThreadContext threadContext;
    private SearchRequest searchRequest;
    private List<Rule> rules;

    @Before
    public void setUpVariables() {
        rules = new ArrayList<>();
        ruleBasedLabelingService = new RuleBasedLabelingService(rules);
        threadContext = new ThreadContext(Settings.EMPTY);
        searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().addLabels(new HashMap<>()));
    }

    public void testConstructorAddsDefaultRule() {
        List<Rule> rules = ruleBasedLabelingService.getRules();
        assertEquals(1, rules.size());
        assertEquals(DefaultUserInfoLabelingRule.class, rules.get(0).getClass());
    }

    public void testAddRule() {
        Rule mockRule = mock(Rule.class);
        ruleBasedLabelingService.addRule(mockRule);
        List<Rule> rules = ruleBasedLabelingService.getRules();
        assertEquals(2, rules.size());
        assertEquals(DefaultUserInfoLabelingRule.class, rules.get(0).getClass());
        assertEquals(mockRule, rules.get(1));
    }

    public void testApplyAllRules() {
        Rule mockRule1 = mock(Rule.class);
        Rule mockRule2 = mock(Rule.class);
        Map<String, Object> labels1 = new HashMap<>();
        labels1.put("label1", "value1");
        Map<String, Object> labels2 = new HashMap<>();
        labels2.put("label2", "value2");
        when(mockRule1.evaluate(threadContext, searchRequest)).thenReturn(labels1);
        when(mockRule2.evaluate(threadContext, searchRequest)).thenReturn(labels2);
        ruleBasedLabelingService.addRule(mockRule1);
        ruleBasedLabelingService.addRule(mockRule2);
        ruleBasedLabelingService.applyAllRules(threadContext, searchRequest);
        Map<String, Object> expectedLabels = new HashMap<>();
        expectedLabels.putAll(labels1);
        expectedLabels.putAll(labels2);
        assertEquals(expectedLabels, searchRequest.source().labels());
    }
}
