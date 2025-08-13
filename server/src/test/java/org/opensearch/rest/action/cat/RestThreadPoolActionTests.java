/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.ThreadPoolType;
import org.opensearch.threadpool.ThreadPoolInfo;
import org.opensearch.threadpool.ThreadPoolStats;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestThreadPoolActionTests extends OpenSearchTestCase {

    public void testBuildTableWithForkJoinPool() throws Exception {
        String nodeId = "node1";
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        when(discoveryNode.getId()).thenReturn(nodeId);
        when(discoveryNode.getName()).thenReturn("test-node");
        when(discoveryNode.getEphemeralId()).thenReturn("eph-id");
        when(discoveryNode.getHostName()).thenReturn("localhost");
        when(discoveryNode.getHostAddress()).thenReturn("127.0.0.1");
        TransportAddress mockTransportAddress = new TransportAddress(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
        when(discoveryNode.getAddress()).thenReturn(mockTransportAddress);

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(discoveryNode).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        ClusterStateResponse clusterStateResponse = mock(ClusterStateResponse.class);
        when(clusterStateResponse.getState()).thenReturn(clusterState);

        ThreadPool.Info forkJoinInfo = new ThreadPool.Info("fork_join", ThreadPoolType.FORK_JOIN);
        List<ThreadPool.Info> threadPoolInfoList = new ArrayList<>();
        threadPoolInfoList.add(forkJoinInfo);

        NodeInfo nodeInfo = mock(NodeInfo.class);
        when(nodeInfo.getNode()).thenReturn(discoveryNode);
        when(nodeInfo.getInfo(ThreadPoolInfo.class)).thenReturn(new ThreadPoolInfo(threadPoolInfoList));
        ProcessInfo processInfo = mock(ProcessInfo.class);
        when(nodeInfo.getInfo(ProcessInfo.class)).thenReturn(processInfo);

        Map<String, NodeInfo> nodesInfoMap = new java.util.HashMap<>();
        nodesInfoMap.put(nodeId, nodeInfo);
        NodesInfoResponse nodesInfoResponse = mock(NodesInfoResponse.class);
        when(nodesInfoResponse.getNodesMap()).thenReturn(nodesInfoMap);

        ThreadPoolStats.Stats forkJoinStats = new ThreadPoolStats.Stats("fork_join", 0, 0, 0, 0, 0, 0, -1);
        List<ThreadPoolStats.Stats> statsList = Collections.singletonList(forkJoinStats);
        ThreadPoolStats threadPoolStats = new ThreadPoolStats(statsList);

        NodeStats nodeStats = mock(NodeStats.class);
        when(nodeStats.getNode()).thenReturn(discoveryNode);
        when(nodeStats.getThreadPool()).thenReturn(threadPoolStats);

        Map<String, NodeStats> nodesStatsMap = new java.util.HashMap<>();
        nodesStatsMap.put(nodeId, nodeStats);
        NodesStatsResponse nodesStatsResponse = mock(NodesStatsResponse.class);
        when(nodesStatsResponse.getNodes()).thenReturn(Collections.singletonList(nodeStats));
        when(nodesStatsResponse.getNodesMap()).thenReturn(nodesStatsMap);

        RestThreadPoolAction action = new RestThreadPoolAction();
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();

        java.lang.reflect.Method buildTableMethod = RestThreadPoolAction.class.getDeclaredMethod(
            "buildTable",
            RestRequest.class,
            ClusterStateResponse.class,
            NodesInfoResponse.class,
            NodesStatsResponse.class
        );
        buildTableMethod.setAccessible(true);
        Object table = buildTableMethod.invoke(action, request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse);

        boolean foundForkJoin = false;
        java.lang.reflect.Method getRowsMethod = table.getClass().getMethod("getRows");
        List<?> rows = (List<?>) getRowsMethod.invoke(table);
        for (Object rowObj : rows) {
            List<?> cells = (List<?>) rowObj;
            List<Object> cellValues = new ArrayList<>();
            for (Object cellObj : cells) {
                java.lang.reflect.Field valueField = cellObj.getClass().getDeclaredField("value");
                valueField.setAccessible(true);
                Object value = valueField.get(cellObj);
                cellValues.add(value);
            }
            if (cellValues.contains("fork_join")) {
                foundForkJoin = true;
                assertEquals("fork_join", cellValues.get(7)); // pool name
                assertEquals(ThreadPoolType.FORK_JOIN.getType(), cellValues.get(8)); // pool type
                assertEquals(0, cellValues.get(9));      // active
                assertEquals(0, cellValues.get(10));     // pool_size
                assertEquals(0, cellValues.get(11));     // queue
                assertEquals(-1, cellValues.get(12));    // queue_size
                assertEquals(0, cellValues.get(13));     // rejected
                assertEquals(0, cellValues.get(14));     // largest
                assertEquals(0, cellValues.get(15));     // completed
                assertEquals(-1, cellValues.get(16));    // total_wait_time
                assertNull(cellValues.get(17));          // core
                assertNull(cellValues.get(18));          // max
                assertNull(cellValues.get(19));          // size
                assertNull(cellValues.get(20));          // keep_alive
            }
        }
        assertTrue("ForkJoinPool row should be present in table", foundForkJoin);
    }
}
