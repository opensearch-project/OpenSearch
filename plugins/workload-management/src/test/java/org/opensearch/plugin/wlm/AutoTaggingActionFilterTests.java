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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugin.wlm.rule.InMemoryRuleProcessingService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.QueryGroupTask;

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
        ruleProcessingService = spy(new InMemoryRuleProcessingService());
        autoTaggingActionFilter = new AutoTaggingActionFilter(ruleProcessingService, threadPool);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testOrder() {
        assertEquals(101, autoTaggingActionFilter.order());
    }

    public void testApplyForValidRequest() {
        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            when(ruleProcessingService.evaluateLabel(anyList()))
                .thenReturn(Optional.of("TestQG_ID"));
            autoTaggingActionFilter.apply(
                mock(Task.class),
                "Test",
                request,
                null, null
                );

            assertEquals("TestQG_ID", threadPool.getThreadContext().getHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER));
            verify(ruleProcessingService, times(1)).evaluateLabel(anyList());
        }
    }

    public void testApplyForInValidRequest() {
        CancelTasksRequest request = new CancelTasksRequest();
        autoTaggingActionFilter.apply(
            mock(Task.class),
            "Test",
            request,
           null, null
        );

        verify(ruleProcessingService, times(0)).evaluateLabel(anyList());
    }
}
