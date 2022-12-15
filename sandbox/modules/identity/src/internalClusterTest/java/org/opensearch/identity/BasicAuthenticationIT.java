/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
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

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

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
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();

        ClusterStateResponse clusterStateResponse = client(clusterManagerNode).admin()
            .cluster()
            .prepareState()
            .setClusterManagerNodeTimeout("1s")
            .clear()
            .setNodes(true)
            .get();
        assertNotNull(clusterStateResponse.getState().nodes().getClusterManagerNodeId());

        // start another node
        final String dataNode = internalCluster().startDataOnlyNode();
        clusterStateResponse = client(dataNode).admin()
            .cluster()
            .prepareState()
            .setClusterManagerNodeTimeout("1s")
            .clear()
            .setNodes(true)
            .setLocal(true)
            .get();
        assertNotNull(clusterStateResponse.getState().nodes().getClusterManagerNodeId());
        // wait for the cluster to form
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(2)).get());
        List<NodeInfo> nodeInfos = client().admin().cluster().prepareNodesInfo().get().getNodes();
        assertEquals(2, nodeInfos.size());

        List<TransportService> transportServices = new ArrayList<TransportService>();
        Map<String, TransportMessageListener> listenerMap = new HashMap<>();

        TransportService clusterManagerService = internalCluster().getInstance(TransportService.class, clusterManagerNode);
        transportServices.add(clusterManagerService);

        TransportService dataNodeService = internalCluster().getInstance(TransportService.class, dataNode);
        transportServices.add(dataNodeService);

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
                }
            };
            listenerMap.put(service.getLocalNode().getId(), listener);
            service.addMessageListener(listener);
        }

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic YWRtaW46YWRtaW4=").build(); // admin:admin
        request.setOptions(options);
        List<NodeInfo> dataNodeInfos = nodeInfos.stream().filter(ni -> ni.getNode().isDataNode()).collect(Collectors.toList());
        RestClient restClient = createRestClient(dataNodeInfos, null, "http");
        Response response = restClient.performRequest(request);

        String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertTrue(interceptedTokens.keySet().size() == 2);
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
    }
}
