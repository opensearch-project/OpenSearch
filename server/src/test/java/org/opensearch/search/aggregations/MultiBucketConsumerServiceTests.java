/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiBucketConsumerServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private CircuitBreaker breaker;
    private WorkloadGroupService workloadGroupService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        breaker = mock(CircuitBreaker.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(MultiBucketConsumerService.MAX_BUCKET_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        workloadGroupService = mock(WorkloadGroupService.class);
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testResolveFallsBackToClusterDefaultWhenNoHeader() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        // No WORKLOAD_GROUP_ID_HEADER in thread context
        assertEquals(100, svc.resolveMaxBuckets());
    }

    public void testResolveFallsBackWhenWorkloadGroupNotFound() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "missing-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(null);
        assertEquals(100, svc.resolveMaxBuckets());
    }

    public void testResolveFallsBackWhenWorkloadGroupHasNoMaxBucketsSetting() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        WorkloadGroup wg = createWorkloadGroup("wg-id", Settings.builder().put("search.default_search_timeout", "30s").build());
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(wg);
        assertEquals(100, svc.resolveMaxBuckets());
    }

    public void testResolveUsesWlmValueWhenSet() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        WorkloadGroup wg = createWorkloadGroup("wg-id", Settings.builder().put("search.max_buckets", "42").build());
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(wg);
        assertEquals(42, svc.resolveMaxBuckets());
    }

    public void testResolveWlmValueOverridesClusterEvenWhenLarger() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        WorkloadGroup wg = createWorkloadGroup("wg-id", Settings.builder().put("search.max_buckets", "10000").build());
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(wg);
        assertEquals(10000, svc.resolveMaxBuckets());
    }

    public void testResolveFallsBackWhenWorkloadGroupServiceNull() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            null
        );
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        assertEquals(100, svc.resolveMaxBuckets());
    }

    public void testResolveFallsBackWhenLookupThrows() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenThrow(new RuntimeException("boom"));
        assertEquals(100, svc.resolveMaxBuckets());
    }

    public void testOverrideRequestValuesIsIrrelevantWhenFalse() {
        // override_request_values=false should NOT prevent the WLM max_buckets from applying,
        // because max_buckets is not a per-request value. The WLM-set value always wins.
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        WorkloadGroup wg = createWorkloadGroup(
            "wg-id",
            Settings.builder().put("search.max_buckets", "42").put("override_request_values", "false").build()
        );
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(wg);
        assertEquals(42, svc.resolveMaxBuckets());
    }

    public void testOverrideRequestValuesIsIrrelevantWhenTrue() {
        // override_request_values=true behaves identically to false for max_buckets — both
        // result in the WLM value being applied. This pins the no-op contract.
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        WorkloadGroup wg = createWorkloadGroup(
            "wg-id",
            Settings.builder().put("search.max_buckets", "42").put("override_request_values", "true").build()
        );
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(wg);
        assertEquals(42, svc.resolveMaxBuckets());
    }

    public void testCreateUsesResolvedLimit() {
        MultiBucketConsumerService svc = new MultiBucketConsumerService(
            clusterService,
            Settings.builder().put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), 100).build(),
            breaker,
            workloadGroupService
        );
        WorkloadGroup wg = createWorkloadGroup("wg-id", Settings.builder().put("search.max_buckets", "7").build());
        threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-id");
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(wg);

        MultiBucketConsumerService.MultiBucketConsumer consumer = svc.create();
        assertEquals(7, consumer.getLimit());
    }

    private WorkloadGroup createWorkloadGroup(String id, Settings searchSettings) {
        return new WorkloadGroup(
            "test-name",
            id,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.MEMORY, 0.5),
                searchSettings
            ),
            System.currentTimeMillis()
        );
    }
}
