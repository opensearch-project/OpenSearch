/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.QueryGroupService;

import static org.mockito.Mockito.mock;

public class WorkloadManagementRequestFailureListenerTests extends OpenSearchTestCase {
    ThreadPool testThreadPool;
    QueryGroupService queryGroupService;
    WorkloadManagementRequestFailureListener sut;

    public void setUp() throws Exception {
        super.setUp();
        testThreadPool = new TestThreadPool("RejectionTestThreadPool");
        queryGroupService = mock(QueryGroupService.class);
        sut = new WorkloadManagementRequestFailureListener(queryGroupService, testThreadPool);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        testThreadPool.shutdown();
    }


}
