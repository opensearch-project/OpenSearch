/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.search.labels.rules.Rule;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestLabelingServiceTests extends OpenSearchTestCase {
    private RequestLabelingService requestLabelingService;
    private ThreadContext threadContext;
    private final ThreadPool threadPool = mock(ThreadPool.class);
    private final Rule mockRule1 = mock(Rule.class);
    private final Rule mockRule2 = mock(Rule.class);
    private final List<Rule> rules = new ArrayList<>();

    @Before
    public void setUpVariables() {
        requestLabelingService = new RequestLabelingService(threadPool, rules);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
    }

    public void testAddRule() {
        Rule mockRule = mock(Rule.class);
        requestLabelingService.addRule(mockRule);
        List<Rule> rules = requestLabelingService.getRules();
        assertEquals(1, rules.size());
        assertEquals(mockRule, rules.get(0));
    }

    public void testGetUserProvidedTag() {
        String expectedTag = "test-tag";
        threadContext.setHeaders(new Tuple<>(Collections.singletonMap(Task.X_OPAQUE_ID, expectedTag), new HashMap<>()));
        String actualTag = requestLabelingService.getUserProvidedTag();
        assertEquals(expectedTag, actualTag);
    }

    public void testBasicApplyAllRules() {
        SearchRequest mockSearchRequest = mock(SearchRequest.class);
        Map<String, Object> mockLabelMap = Collections.singletonMap("label1", "value1");
        when(mockRule1.evaluate(threadContext, mockSearchRequest)).thenReturn(mockLabelMap);
        rules.add(mockRule1);
        requestLabelingService.applyAllRules(mockSearchRequest);
        Map<String, Object> computedLabels = threadContext.getTransient(RequestLabelingService.COMPUTED_LABELS);
        assertEquals(1, computedLabels.size());
        assertEquals("value1", computedLabels.get("label1"));
    }

    public void testApplyAllRulesWithConflict() {
        SearchRequest mockSearchRequest = mock(SearchRequest.class);
        Map<String, Object> mockLabelMap1 = Collections.singletonMap("conflictingLabel", "value1");
        Map<String, Object> mockLabelMap2 = Collections.singletonMap("conflictingLabel", "value2");
        when(mockRule1.evaluate(threadContext, mockSearchRequest)).thenReturn(mockLabelMap1);
        when(mockRule2.evaluate(threadContext, mockSearchRequest)).thenReturn(mockLabelMap2);
        rules.add(mockRule1);
        rules.add(mockRule2);
        requestLabelingService.applyAllRules(mockSearchRequest);
        Map<String, Object> computedLabels = threadContext.getTransient(RequestLabelingService.COMPUTED_LABELS);
        assertEquals(1, computedLabels.size());
        assertEquals("value2", computedLabels.get("conflictingLabel"));
    }

    public void testApplyAllRulesWithoutConflict() {
        SearchRequest mockSearchRequest = mock(SearchRequest.class);
        Map<String, Object> mockLabelMap1 = Collections.singletonMap("label1", "value1");
        Map<String, Object> mockLabelMap2 = Collections.singletonMap("label2", "value2");
        when(mockRule1.evaluate(threadContext, mockSearchRequest)).thenReturn(mockLabelMap1);
        when(mockRule2.evaluate(threadContext, mockSearchRequest)).thenReturn(mockLabelMap2);
        rules.add(mockRule1);
        rules.add(mockRule2);
        requestLabelingService.applyAllRules(mockSearchRequest);
        Map<String, Object> computedLabels = threadContext.getTransient(RequestLabelingService.COMPUTED_LABELS);
        assertEquals(2, computedLabels.size());
        assertEquals("value1", computedLabels.get("label1"));
        assertEquals("value2", computedLabels.get("label2"));
    }
}
