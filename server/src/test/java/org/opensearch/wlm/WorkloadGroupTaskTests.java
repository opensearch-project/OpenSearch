/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;

import static org.opensearch.wlm.WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER;
import static org.opensearch.wlm.WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER;

public class WorkloadGroupTaskTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private WorkloadGroupTask sut;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        sut = new WorkloadGroupTask(123, "transport", "Search", "test task", null, Collections.emptyMap());
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testSuccessfulSetWorkloadGroupId() {
        sut.setWorkloadGroupId(threadPool.getThreadContext());
        assertEquals(DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get(), sut.getWorkloadGroupId());

        threadPool.getThreadContext().putHeader(WORKLOAD_GROUP_ID_HEADER, "akfanglkaglknag2332");

        sut.setWorkloadGroupId(threadPool.getThreadContext());
        assertEquals("akfanglkaglknag2332", sut.getWorkloadGroupId());
    }

    public void testThrottlePrincipalIsPerTaskAndNotAHeader() {
        assertNull("an absent principal must read as null so username/role throttling fails open", sut.getThrottlePrincipal());

        sut.setThrottlePrincipal("username|alice");
        assertEquals("username|alice", sut.getThrottlePrincipal());

        // _msearch sub-requests share a thread context, so the principal is per task to avoid cross-billing.
        WorkloadGroupTask other = new WorkloadGroupTask(124, "transport", "Search", "test task", null, Collections.emptyMap());
        assertNull(other.getThrottlePrincipal());

        // Not in the header maps, which are serialized to every shard and remote cluster.
        assertNull(sut.getHeader("workloadGroupPrincipal"));
        assertNull(threadPool.getThreadContext().getHeader("workloadGroupPrincipal"));
    }
}
