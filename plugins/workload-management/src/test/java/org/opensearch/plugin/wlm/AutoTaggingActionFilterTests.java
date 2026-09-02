/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WorkloadGroupTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.plugin.wlm.WorkloadManagementPlugin.PRINCIPAL_ATTRIBUTE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doAnswer;
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
        AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
            WLMFeatureType.WLM,
            DefaultAttributeValueStore::new
        );
        ruleProcessingService = spy(new InMemoryRuleProcessingService(attributeValueStoreFactory, null));
        autoTaggingActionFilter = new AutoTaggingActionFilter(
            ruleProcessingService,
            threadPool,
            new HashMap<>(),
            mock(WlmClusterSettingValuesProvider.class),
            WLMFeatureType.WLM
        );
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
        ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("TestQG_ID"));
            autoTaggingActionFilter.apply(mock(Task.class), "Test", request, ActionRequestMetadata.empty(), null, mockFilterChain);

            assertEquals("TestQG_ID", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
            verify(ruleProcessingService, times(1)).evaluateLabel(anyList());
        }
    }

    public void testApplyForInValidRequest() {
        ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
        CancelTasksRequest request = new CancelTasksRequest();
        autoTaggingActionFilter.apply(mock(Task.class), "Test", request, ActionRequestMetadata.empty(), null, mockFilterChain);

        verify(ruleProcessingService, times(0)).evaluateLabel(anyList());
    }

    public void testApplySetsThrottlePrincipalOnTaskWhenExtractorPresent() {
        // A feature type that includes a "principal" attribute + a matching extractor extension in the map.
        Attribute principalAttr = new Attribute() {
            @Override
            public String getName() {
                return PRINCIPAL_ATTRIBUTE_NAME;
            }

            @Override
            public void validateAttribute() {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {}
        };
        FeatureType featureTypeWithPrincipal = new FeatureType() {
            @Override
            public String getName() {
                return "wlm";
            }

            @Override
            public Map<Attribute, Integer> getOrderedAttributes() {
                return Map.of(principalAttr, 1);
            }
        };
        AttributeExtractor<String> principalExtractor = new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return principalAttr;
            }

            @Override
            public Iterable<String> extract() {
                return List.of("username|alice", "role|admin");
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return LogicalOperator.OR;
            }
        };
        AttributeExtractorExtension extension = () -> principalExtractor;
        Map<Attribute, AttributeExtractorExtension> extensions = Map.of(principalAttr, extension);

        InMemoryRuleProcessingService svc = spy(
            new InMemoryRuleProcessingService(
                new AttributeValueStoreFactory(featureTypeWithPrincipal, DefaultAttributeValueStore::new),
                null
            )
        );
        AutoTaggingActionFilter filter = new AutoTaggingActionFilter(
            svc,
            threadPool,
            extensions,
            mock(WlmClusterSettingValuesProvider.class),
            featureTypeWithPrincipal
        );

        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);
        WorkloadGroupTask task = newWorkloadGroupTask();
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            when(svc.evaluateLabel(anyList())).thenReturn(Optional.of("QG"));
            filter.apply(task, "Test", request, ActionRequestMetadata.empty(), null, chain);

            // Both principal tokens are joined (by WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER) onto the task for
            // core-side throttling.
            assertEquals(
                "username|alice" + WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER + "role|admin",
                task.getThrottlePrincipal()
            );
            // The principal must NOT land in the thread context: request headers are serialized onto every outgoing
            // transport request, which would ship the caller's identity to every shard and to remote clusters.
            assertNull(threadPool.getThreadContext().getHeader("workloadGroupPrincipal"));
        }
    }

    public void testApplyTwiceOnOneThreadContextIsTolerated() {
        // The filter can run more than once against one ThreadContext, and ThreadContext.putHeader throws when the key is
        // already present -- so anything the filter writes there must tolerate a repeat, or the second run fails the
        // request. Carrying the principal on the task instead is what makes that safe, and gives each sub-request its own
        // value.
        //
        // Note the repeat is not the ordinary _msearch dispatch loop: TransportAction.execute takes
        // taskManager.taskExecutionStarted(task) and closes it in a finally, which restores the request headers between
        // sub-searches. It happens when a sub-search is dispatched from inside a previous one's response handling -- the
        // queue drain once numRequests exceeds max_concurrent_searches -- where the sender's context, header included, is
        // the one restored.
        Attribute principalAttr = new Attribute() {
            @Override
            public String getName() {
                return PRINCIPAL_ATTRIBUTE_NAME;
            }

            @Override
            public void validateAttribute() {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {}
        };
        FeatureType featureTypeWithPrincipal = new FeatureType() {
            @Override
            public String getName() {
                return "wlm";
            }

            @Override
            public Map<Attribute, Integer> getOrderedAttributes() {
                return Map.of(principalAttr, 1);
            }
        };
        AtomicInteger extractCalls = new AtomicInteger();
        AttributeExtractor<String> principalExtractor = new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return principalAttr;
            }

            @Override
            public Iterable<String> extract() {
                extractCalls.incrementAndGet();
                return List.of("username|alice");
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return LogicalOperator.OR;
            }
        };
        AttributeExtractorExtension extension = () -> principalExtractor;
        InMemoryRuleProcessingService svc = spy(
            new InMemoryRuleProcessingService(
                new AttributeValueStoreFactory(featureTypeWithPrincipal, DefaultAttributeValueStore::new),
                null
            )
        );
        AutoTaggingActionFilter filter = new AutoTaggingActionFilter(
            svc,
            threadPool,
            Map.of(principalAttr, extension),
            mock(WlmClusterSettingValuesProvider.class),
            featureTypeWithPrincipal
        );

        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);
        WorkloadGroupTask first = newWorkloadGroupTask();
        WorkloadGroupTask second = newWorkloadGroupTask();
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            // No label, so the (separate, pre-existing) workload-group-id header is not set and this test isolates the
            // principal.
            when(svc.evaluateLabel(anyList())).thenReturn(Optional.empty());
            filter.apply(first, "Test", request, ActionRequestMetadata.empty(), null, chain);
            filter.apply(second, "Test", request, ActionRequestMetadata.empty(), null, chain);

            // Each sub-request carries its own principal, and neither run threw on a duplicate key.
            assertEquals("username|alice", first.getThrottlePrincipal());
            assertEquals("username|alice", second.getThrottlePrincipal());
            assertNull(threadPool.getThreadContext().getHeader("workloadGroupPrincipal"));
            // The principal is materialized once per request and reused for both label evaluation and the task field;
            // extract() carries no re-iterability contract, so calling it twice per request risks yielding nothing.
            assertEquals("extract() must be invoked once per request", 2, extractCalls.get());
        }
    }

    public void testApplyLeavesThrottlePrincipalUnsetWhenNoExtractor() {
        // Default filter from setUp has no principal attribute/extractor -> the task's principal stays null, which is
        // what makes username/role throttling fail open rather than bucket everyone together.
        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("QG"));
            WorkloadGroupTask task = newWorkloadGroupTask();
            autoTaggingActionFilter.apply(task, "Test", request, ActionRequestMetadata.empty(), null, chain);
            assertNull(task.getThrottlePrincipal());
        }
    }

    public void testApplyForScrollRequestWithOriginalIndices() {
        SearchScrollRequest request = mock(SearchScrollRequest.class);
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);

        @SuppressWarnings("unchecked")
        ActionRequestMetadata<ActionRequest, ActionResponse> metadata = mock(ActionRequestMetadata.class);
        when(request.originalIndicesOrEmpty()).thenReturn(new String[] { "logs-scroll-index" });

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            doAnswer(inv -> {
                @SuppressWarnings("unchecked")
                List<AttributeExtractor<String>> extractors = inv.getArgument(0);

                assertNotNull(extractors);
                assertEquals(1, extractors.size());

                AttributeExtractor<String> ex = extractors.get(0);
                assertEquals(RuleAttribute.INDEX_PATTERN, ex.getAttribute());

                List<String> values = new ArrayList<>();
                ex.extract().forEach(values::add);
                assertEquals(List.of("logs-scroll-index"), values);

                return Optional.of("ScrollQG_ID");
            }).when(ruleProcessingService).evaluateLabel(any());

            autoTaggingActionFilter.apply(mock(Task.class), "Test", request, metadata, null, chain);

            assertEquals("ScrollQG_ID", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
            verify(ruleProcessingService, times(1)).evaluateLabel(anyList());
        }
    }

    public enum WLMFeatureType implements FeatureType {
        WLM;

        @Override
        public String getName() {
            return "";
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return Map.of(TestAttribute.TEST_ATTRIBUTE, 1);
        }
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

    private static WorkloadGroupTask newWorkloadGroupTask() {
        return new WorkloadGroupTask(1L, "transport", "Test", "test task", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static class TestActionFilterChain implements ActionFilterChain<ActionRequest, ActionResponse> {
        @Override
        public void proceed(Task task, String action, ActionRequest request, ActionListener<ActionResponse> listener) {

        }
    }
}
