/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BasicAuthenticationIT extends HttpSmokeTestCaseWithIdentity {

    public static Map<String, String> interceptedTokens = new HashMap<>();
    private final static String expectedActionName = "cluster:monitor/health";

    public static class TokenInterceptorPlugin extends Plugin implements NetworkPlugin {
        public TokenInterceptorPlugin() {}

        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return Arrays.asList(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new AsyncSender() {
                        @Override
                        public <T extends TransportResponse> void sendRequest(
                            Transport.Connection connection,
                            String action,
                            TransportRequest request,
                            TransportRequestOptions options,
                            TransportResponseHandler<T> handler
                        ) {

                            Map<String, String> tcHeaders = threadContext.getHeaders();
                            if (expectedActionName.equals(action)) {
                                if (tcHeaders.containsKey(ThreadContextConstants.OPENSEARCH_AUTHENTICATION_TOKEN_HEADER)) {
                                    interceptedTokens.put(
                                        request.getParentTask().getNodeId(),
                                        tcHeaders.get(ThreadContextConstants.OPENSEARCH_AUTHENTICATION_TOKEN_HEADER)
                                    );
                                }
                            }
                            // String prefix = "(nodeName=" + request.getParentTask().getNodeId() + ", requestId=" +
                            // request.getParentTask().getId() + ", action=" + action + " interceptSender)";
                            // System.out.println(prefix + " Headers: " + threadContext.getHeaders());
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    };
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = super.nodePlugins().stream().collect(Collectors.toList());
        plugins.add(TokenInterceptorPlugin.class);
        return plugins;
    }

    public void testBasicAuth() throws Exception {
        final List<String> nodes = internalCluster().startNodes(2);
        ensureStableCluster(2);

        List<TransportService> transportServices = new ArrayList<TransportService>();
        Map<String, TransportMessageListener> listenerMap = new HashMap<>();
        for (String nodeName : nodes) {
            interceptedTokens.put(internalCluster().clusterService().localNode().getId(), null);
            TransportService service = internalCluster().getInstance(TransportService.class, nodeName);
            transportServices.add(service);
        }

        String expectedActionName = "cluster:monitor/health";

        for (TransportService service : transportServices) {
            TransportMessageListener listener = new TransportMessageListener() {
                @Override
                public void onRequestReceived(long requestId, String action) {
                    final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, service.getLocalNode().getName());
                    Map<String, String> tcHeaders = threadPool.getThreadContext().getHeaders();
                    if (expectedActionName.equals(action)) {
                        if (tcHeaders.containsKey(ThreadContextConstants.OPENSEARCH_AUTHENTICATION_TOKEN_HEADER)) {
                            interceptedTokens.put(
                                service.getLocalNode().getId(),
                                tcHeaders.get(ThreadContextConstants.OPENSEARCH_AUTHENTICATION_TOKEN_HEADER)
                            );
                        }
                    }
                    // String prefix = "(nodeName=" + service.getLocalNode().getId() + ", requestId=" + requestId + ", action=" + action + "
                    // onRequestReceived)";
                    // System.out.println(prefix + " Headers: " + threadPool.getThreadContext().getHeaders());
                }
            };
            listenerMap.put(service.getLocalNode().getId(), listener);
            service.addMessageListener(listener);
        }

        Thread.sleep(2000);

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic YWRtaW46YWRtaW4=").build(); // admin:admin
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);

        String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        // System.out.println("interceptedTokens: " + interceptedTokens);

        assertFalse(interceptedTokens.values().stream().anyMatch(s -> Objects.isNull(s)));

        List<String> tokens = interceptedTokens.values().stream().collect(Collectors.toList());

        boolean allEqual = tokens.isEmpty() || tokens.stream().allMatch(tokens.get(0)::equals);
        assertTrue(allEqual);

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertTrue(content.contains("\"status\":\"green\""));

        for (TransportService service : transportServices) {
            service.removeMessageListener(listenerMap.get(service.getLocalNode().getId()));
        }
        interceptedTokens = null;
        ensureStableCluster(2);
    }
}
