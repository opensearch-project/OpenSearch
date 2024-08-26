/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.QueryGroupTask;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryGroupRequestRejectionOperationListenerTests extends OpenSearchTestCase {
    ThreadPool testThreadPool;
    QueryGroupService queryGroupService;
    QueryGroupRequestRejectionOperationListener sut;


    public void setUp() throws Exception {
        super.setUp();
        testThreadPool = new TestThreadPool("RejectionTestThreadPool");
    }

    public void tearDown() throws Exception {
        super.tearDown();
        testThreadPool.shutdown();
    }

    public void testRejectionCase() {
        queryGroupService = mock(QueryGroupService.class);
        sut = new QueryGroupRequestRejectionOperationListener(queryGroupService, testThreadPool);
        final String testQueryGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, testQueryGroupId);
        when(queryGroupService.shouldRejectFor(testQueryGroupId)).thenReturn(Optional.of("Test query group is contended"));

        assertThrows(OpenSearchRejectedExecutionException.class, () -> sut.onRequestStart(null));
    }

    public void testNonRejectionCase() {
        queryGroupService = mock(QueryGroupService.class);
        sut = new QueryGroupRequestRejectionOperationListener(queryGroupService, testThreadPool);
        final String testQueryGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, testQueryGroupId);
        when(queryGroupService.shouldRejectFor(testQueryGroupId)).thenReturn(Optional.empty());

        sut.onRequestStart(null);
    }
}
