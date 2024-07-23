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
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import static org.opensearch.threadpool.ThreadPool.Names.SAME;

public class SearchWorkloadTransportInterceptorTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private SearchWorkloadTransportInterceptor sut;

    public void setUp() throws Exception {
        threadPool = new TestThreadPool(getTestName());
        sut = new SearchWorkloadTransportInterceptor(threadPool);
    }

    public void tearDown() throws Exception {
        threadPool.shutdown();
    }

    public void testInterceptHandler() {
        TransportRequestHandler<TransportRequest> requestHandler = sut.interceptHandler("Search", SAME, false, null);
        assertTrue(requestHandler instanceof SearchWorkloadTransportHandler);
    }
}
