/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.QueryGroupTask;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoTaggingActionFilterTests extends OpenSearchTestCase {

    AutoTaggingActionFilter autoTaggingActionFilter;
    InMemoryRuleProcessingService ruleProcessingService;
    ThreadPool threadPool;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("AutoTaggingActionFilterTests");
        ruleProcessingService = spy(new InMemoryRuleProcessingService(WLMFeatureType.WLM, DefaultAttributeValueStore::new));
        autoTaggingActionFilter = new AutoTaggingActionFilter(ruleProcessingService, threadPool);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testOrder() {
        assertEquals(Integer.MAX_VALUE, autoTaggingActionFilter.order());
    }

    public void testApplyForValidRequest() {
        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("TestQG_ID"));
            autoTaggingActionFilter.apply(mock(Task.class), "Test", request, null, null);

            assertEquals("TestQG_ID", threadPool.getThreadContext().getHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER));
            verify(ruleProcessingService, times(1)).evaluateLabel(anyList());
        }
    }

    public void testApplyForInValidRequest() {
        CancelTasksRequest request = new CancelTasksRequest();
        autoTaggingActionFilter.apply(mock(Task.class), "Test", request, null, null);

        verify(ruleProcessingService, times(0)).evaluateLabel(anyList());
    }

    public enum WLMFeatureType implements FeatureType {
        WLM;

        @Override
        public String getName() {
            return "";
        }

        @Override
        public Map<String, Attribute> getAllowedAttributesRegistry() {
            return Map.of("test_attribute", TestAttribute.TEST_ATTRIBUTE);
        }

        @Override
        public void registerFeatureType() {}
    }

    public enum TestAttribute implements Attribute {
        TEST_ATTRIBUTE("test_attribute"),
        INVALID_ATTRIBUTE("invalid_attribute");

        private final String name;

        TestAttribute(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void validateAttribute() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
