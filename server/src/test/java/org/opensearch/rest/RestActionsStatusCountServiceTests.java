/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.BeforeClass;
import org.opensearch.action.admin.cluster.node.stats.RestActionsStats;
import org.opensearch.client.node.NodeClient;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class RestActionsStatusCountServiceTests extends OpenSearchTestCase {
    private static List<HandlerStatusMap> handlerStatusMaps;

    @BeforeClass
    public static void setup() {
        handlerStatusMaps = List.of(
            new HandlerStatusMap("handler_a", RestStatus.OK),
            new HandlerStatusMap("handler_a", RestStatus.BAD_REQUEST),
            new HandlerStatusMap("handler_b", RestStatus.OK),
            new HandlerStatusMap("handler_c", RestStatus.OK),
            new HandlerStatusMap("handler_d", RestStatus.INTERNAL_SERVER_ERROR),
            new HandlerStatusMap("handler_d", RestStatus.BAD_REQUEST)
        );
    }

    public void testHandlerCanNotBeNull() {
        final RestActionsStatusCountService service = new RestActionsStatusCountService();
        expectThrows(NullPointerException.class, () -> service.addRestHandler(null));
    }

    public void testHandlerWithNoName() {
        final RestActionsStatusCountService service = new RestActionsStatusCountService();
        final BaseRestHandler horse = new MockRestHandler(null);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.addRestHandler(horse));
        assertThat(
            e.getMessage(),
            equalTo("handler of type [org.opensearch.rest.RestActionsStatusCountServiceTests$MockRestHandler] does not have a name")
        );
    }

    public void testRestActionsServiceWithoutFilters() {
        RestActionsStatusCountService service = new RestActionsStatusCountService();
        // adding handlers and it's response status to service
        addHandlerAndStatus(service, handlerStatusMaps);
        RestActionsStats stats = service.stats(null);
        final Map<String, Map<Integer, AtomicLong>> handlerStatusCount = stats.getRestActionsStatusCount();
        final Map<Integer, AtomicLong> totalStatusCount = stats.getRestActionsStatusTotalCount();
        assertTrue(handlerStatusCount != null);
        assertTrue(totalStatusCount != null);
        assertTrue(handlerStatusCount.get("handler_a").size() == 2);
        assertTrue(handlerStatusCount.get("handler_b").size() == 1);
        assertTrue(handlerStatusCount.get("handler_c").size() == 1);
        assertTrue(handlerStatusCount.get("handler_d").size() == 2);

        assertTrue(totalStatusCount.get(RestStatus.OK.getStatus()).longValue() == 3);
        assertTrue(totalStatusCount.get(RestStatus.BAD_REQUEST.getStatus()).longValue() == 2);
        assertTrue(totalStatusCount.get(RestStatus.INTERNAL_SERVER_ERROR.getStatus()).longValue() == 1);
    }

    public void testRestActionsServiceWithFilters() {
        final RestActionsStatusCountService service = new RestActionsStatusCountService();
        // adding handlers and it's response status to service
        addHandlerAndStatus(service, handlerStatusMaps);
        RestActionsStats stats = service.stats(Set.of("handler_a", "handler_d"));
        final Map<String, Map<Integer, AtomicLong>> handlerStatusCount = stats.getRestActionsStatusCount();
        final Map<Integer, AtomicLong> totalStatusCount = stats.getRestActionsStatusTotalCount();
        assertTrue(handlerStatusCount != null);
        assertTrue(totalStatusCount != null);
        assertTrue(handlerStatusCount.get("handler_a").size() == 2);
        assertTrue(handlerStatusCount.get("handler_b") == null);
        assertTrue(handlerStatusCount.get("handler_c") == null);
        assertTrue(handlerStatusCount.get("handler_d").size() == 2);

        assertTrue(totalStatusCount.get(RestStatus.OK.getStatus()).longValue() == 1);
        assertTrue(totalStatusCount.get(RestStatus.BAD_REQUEST.getStatus()).longValue() == 2);
        assertTrue(totalStatusCount.get(RestStatus.INTERNAL_SERVER_ERROR.getStatus()).longValue() == 1);
    }

    /**
     * Test to ensure consistency of count in case of multi-threading
     * @throws Exception
     */
    public void testRestActionsServiceForMultithreading() throws Exception {
        final RestActionsStatusCountService service = new RestActionsStatusCountService();
        // adding only handlers
        addHandlers(service, handlerStatusMaps);
        final int MAX_THREADS = RandomizedTest.randomIntBetween(100, 300);
        final RestStatus restStatus = RestStatus.OK;
        final Set<String> handlers = service.getHandlers().keySet();
        final int MAX_HANDLERS = handlers.size();
        final Thread[] threads = new Thread[MAX_THREADS * MAX_HANDLERS];
        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < threads.length; i++) {
            for (String handler : handlers) {
                threads[i] = new Thread(() -> {
                    try {
                        latch.await();
                        service.putStatus(handler, restStatus);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[i].start();
            }
        }
        latch.countDown();
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        RestActionsStats stats = service.stats(null);
        Map<String, Map<Integer, AtomicLong>> restActionsStatusCount = stats.getRestActionsStatusCount();
        Map<Integer, AtomicLong> restActionsStatusTotalCount = stats.getRestActionsStatusTotalCount();

        int total = 0;
        for (Map.Entry<String, Map<Integer, AtomicLong>> entry : restActionsStatusCount.entrySet()) {
            int count = entry.getValue().get(restStatus.getStatus()).intValue();
            assertEquals(threads.length, count);
            total += count;
        }

        assertEquals(total, restActionsStatusTotalCount.get(restStatus.getStatus()).intValue());
    }

    private static void addHandlers(final RestActionsStatusCountService service, final List<HandlerStatusMap> handlerStatusMaps) {
        handlerStatusMaps.stream().forEach(entry -> service.addRestHandler(new MockRestHandler(entry.getHandler())));
    }

    private static final class HandlerStatusMap {
        private String handler;
        private RestStatus status;

        public HandlerStatusMap(String handler, RestStatus status) {
            this.handler = handler;
            this.status = status;
        }

        public String getHandler() {
            return handler;
        }

        public RestStatus getStatus() {
            return status;
        }
    }

    private static void addHandlerAndStatus(final RestActionsStatusCountService service, final List<HandlerStatusMap> handlerStatusMaps) {
        handlerStatusMaps.stream().forEach(entry -> {
            service.addRestHandler(new MockRestHandler(entry.getHandler()));
            service.putStatus(entry.getHandler(), entry.getStatus());
        });
    }

    private static final class MockRestHandler extends BaseRestHandler {
        private String name;

        protected MockRestHandler(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            return channel -> {};
        }
    }
}
