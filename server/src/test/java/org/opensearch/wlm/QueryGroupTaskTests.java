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

import static org.opensearch.wlm.QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER;
import static org.opensearch.wlm.QueryGroupTask.QUERY_GROUP_ID_HEADER;

public class QueryGroupTaskTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private QueryGroupTask sut;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        sut = new QueryGroupTask(123, "transport", "Search", "test task", null, Collections.emptyMap());
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testSuccessfulSetQueryGroupId() {
        sut.setQueryGroupId(threadPool.getThreadContext());
        assertEquals(DEFAULT_QUERY_GROUP_ID_SUPPLIER.get(), sut.getQueryGroupId());

        threadPool.getThreadContext().putHeader(QUERY_GROUP_ID_HEADER, "akfanglkaglknag2332");

        sut.setQueryGroupId(threadPool.getThreadContext());
        assertEquals("akfanglkaglknag2332", sut.getQueryGroupId());
    }
}
