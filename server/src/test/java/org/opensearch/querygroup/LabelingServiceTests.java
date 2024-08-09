/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querygroup;

import org.opensearch.action.IndicesRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.LabelingPlugin;
import org.opensearch.querygroup.LabelingService.LabelingImplementationType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

import static org.mockito.Mockito.mock;

public class LabelingServiceTests extends OpenSearchTestCase {

    ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("QSB");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testInvalidInstantiationOfLabelingService() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new LabelingService(
                List.of(
                    getTestImplementation("", "", LabelingImplementationType.QUERY_GROUP_RESOURCE_MANAGEMENT),
                    getTestImplementation("", "", LabelingImplementationType.QUERY_GROUP_RESOURCE_MANAGEMENT)
                )
            )
        );
    }

    public void testExistingImplementationExistingCase() {
        LabelingService labelingService = new LabelingService(
            List.of(
                getTestImplementation("queryGroupId", "akfagagnaga232_2434t", LabelingImplementationType.QUERY_GROUP_RESOURCE_MANAGEMENT)
            )
        );
        IndicesRequest request = mock(IndicesRequest.class);
        // threadPool = new TestThreadPool("QSB");
        ThreadContext threadContext = threadPool.getThreadContext();

        labelingService.labelRequestFor(LabelingImplementationType.QUERY_GROUP_RESOURCE_MANAGEMENT, request, threadContext);
        assertEquals(threadContext.getHeader("queryGroupId"), "akfagagnaga232_2434t");
    }

    public void testNonExistingImplementationExistingCase() {
        LabelingService labelingService = new LabelingService(
            List.of(
                getTestImplementation("queryGroupId", "akfagagnaga232_2434t", LabelingImplementationType.QUERY_GROUP_RESOURCE_MANAGEMENT)
            )
        );
        IndicesRequest request = mock(IndicesRequest.class);
        // threadPool = new TestThreadPool("QSB");
        ThreadContext threadContext = threadPool.getThreadContext();

        assertThrows(
            IllegalArgumentException.class,
            () -> labelingService.labelRequestFor(LabelingImplementationType.NOOP, request, threadContext)
        );

    }

    LabelingPlugin getTestImplementation(String header, String value, LabelingImplementationType type) {
        return new LabelingPlugin() {
            @Override
            public LabelingImplementationType getImplementationName() {
                return type;
            }

            @Override
            public void labelRequest(IndicesRequest request, ThreadContext threadContext) {
                threadContext.putHeader(header, value);
            }
        };
    }
}
