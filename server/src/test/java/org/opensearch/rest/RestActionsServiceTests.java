/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.junit.BeforeClass;
import org.opensearch.action.admin.cluster.node.stats.RestActionsStats;
import org.opensearch.client.node.NodeClient;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class RestActionsServiceTests extends OpenSearchTestCase {
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
        final RestActionsService service = new RestActionsService();
        expectThrows(NullPointerException.class, () -> service.addRestHandler(null));
    }

    public void testHandlerWithNoName() {
        final RestActionsService service = new RestActionsService();
        final BaseRestHandler horse = new MockRestHandler(null);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.addRestHandler(horse));
        assertThat(
            e.getMessage(),
            equalTo("handler of type [org.opensearch.rest.RestActionsServiceTests$MockRestHandler] does not have a name")
        );
    }

    public void testRestActionsServiceWithoutFilters() {
        RestActionsService service = new RestActionsService();
        // adding handlers and it's response status to service
        addHandlerAndStatus(service, handlerStatusMaps);
        RestActionsStats stats = service.stats(null);
        final Map<String, Map<Integer, Integer>> handlerStatusCount = stats.getRestActionsStatusCount();
        final Map<Integer, Integer> totalStatusCount = stats.getRestActionsStatusTotalCount();
        assertTrue(handlerStatusCount != null);
        assertTrue(totalStatusCount != null);
        assertTrue(handlerStatusCount.get("handler_a").size() == 2);
        assertTrue(handlerStatusCount.get("handler_b").size() == 1);
        assertTrue(handlerStatusCount.get("handler_c").size() == 1);
        assertTrue(handlerStatusCount.get("handler_d").size() == 2);

        assertTrue(totalStatusCount.get(RestStatus.OK.getStatus()) == 3);
        assertTrue(totalStatusCount.get(RestStatus.BAD_REQUEST.getStatus()) == 2);
        assertTrue(totalStatusCount.get(RestStatus.INTERNAL_SERVER_ERROR.getStatus()) == 1);
    }

    public void testRestActionsServiceWithFilters() {
        final RestActionsService service = new RestActionsService();
        // adding handlers and it's response status to service
        addHandlerAndStatus(service, handlerStatusMaps);
        RestActionsStats stats = service.stats(Set.of("handler_a", "handler_d"));
        final Map<String, Map<Integer, Integer>> handlerStatusCount = stats.getRestActionsStatusCount();
        final Map<Integer, Integer> totalStatusCount = stats.getRestActionsStatusTotalCount();
        assertTrue(handlerStatusCount != null);
        assertTrue(totalStatusCount != null);
        assertTrue(handlerStatusCount.get("handler_a").size() == 2);
        assertTrue(handlerStatusCount.get("handler_b") == null);
        assertTrue(handlerStatusCount.get("handler_c") == null);
        assertTrue(handlerStatusCount.get("handler_d").size() == 2);

        assertTrue(totalStatusCount.get(RestStatus.OK.getStatus()) == 1);
        assertTrue(totalStatusCount.get(RestStatus.BAD_REQUEST.getStatus()) == 2);
        assertTrue(totalStatusCount.get(RestStatus.INTERNAL_SERVER_ERROR.getStatus()) == 1);
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

    private static void addHandlerAndStatus(final RestActionsService service, final List<HandlerStatusMap> handlerStatusMaps) {
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
