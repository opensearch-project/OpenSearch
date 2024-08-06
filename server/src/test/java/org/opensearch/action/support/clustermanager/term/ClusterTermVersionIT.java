/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterTermVersionIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    public void testClusterStateResponseFromDataNode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        ensureClusterSizeConsistency();
        ensureGreen();

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForTimeout(TimeValue.timeValueHours(1));
        ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(clusterStateRequest).get();
        assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
        assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));
        assertThat(stateResponse.isWaitForTimedOut(), is(false));

    }

    public void testClusterStateResponseFromClusterManagerNode() throws Exception {
        String master = internalCluster().startClusterManagerOnlyNode();
        String data = internalCluster().startDataOnlyNode();
        ensureClusterSizeConsistency();
        ensureGreen();
        Map<String, AtomicInteger> callCounters = Map.ofEntries(
            Map.entry(ClusterStateAction.NAME, new AtomicInteger()),
            Map.entry(GetTermVersionAction.NAME, new AtomicInteger())
        );

        addCallCountInterceptor(master, callCounters);

        ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(new ClusterStateRequest()).get();

        AtomicInteger clusterStateCallsOnMaster = callCounters.get(ClusterStateAction.NAME);
        AtomicInteger termCallsOnMaster = callCounters.get(GetTermVersionAction.NAME);

        assertThat(clusterStateCallsOnMaster.get(), is(0));
        assertThat(termCallsOnMaster.get(), is(1));

        assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
        assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));

    }

    public void testDatanodeOutOfSync() throws Exception {
        String master = internalCluster().startClusterManagerOnlyNode();
        String data = internalCluster().startDataOnlyNode();
        ensureClusterSizeConsistency();
        ensureGreen();
        Map<String, AtomicInteger> callCounters = Map.ofEntries(
            Map.entry(ClusterStateAction.NAME, new AtomicInteger()),
            Map.entry(GetTermVersionAction.NAME, new AtomicInteger())
        );

        stubClusterTermResponse(master);
        addCallCountInterceptor(master, callCounters);

        ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(new ClusterStateRequest()).get();

        AtomicInteger clusterStateCallsOnMaster = callCounters.get(ClusterStateAction.NAME);
        AtomicInteger termCallsOnMaster = callCounters.get(GetTermVersionAction.NAME);

        assertThat(clusterStateCallsOnMaster.get(), is(1));
        assertThat(termCallsOnMaster.get(), is(1));

        assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
        assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));
    }

    private void addCallCountInterceptor(String nodeName, Map<String, AtomicInteger> callCounters) {
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
        for (var ctrEnty : callCounters.entrySet()) {
            primaryService.addRequestHandlingBehavior(ctrEnty.getKey(), (handler, request, channel, task) -> {
                ctrEnty.getValue().incrementAndGet();
                logger.info("-->  {} response redirect", ClusterStateAction.NAME);
                handler.messageReceived(request, channel, task);
            });
        }
    }

    private void stubClusterTermResponse(String master) {
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(TransportService.class, master);
        primaryService.addRequestHandlingBehavior(GetTermVersionAction.NAME, (handler, request, channel, task) -> {
            channel.sendResponse(new GetTermVersionResponse(new ClusterStateTermVersion(new ClusterName("test"), "1", -1, -1)));
        });
    }

}
