/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.rest.action.cat;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Table;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolInfo;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.client.node.NodeClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestThreadPoolActionRowTests extends OpenSearchTestCase {

    private RestThreadPoolAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestThreadPoolAction();
    }

    public void testForkJoinRow() {
        final String nodeName = "n1";
        final String nodeId = "id1";
        final String eid = "e1";
        final Long pid = 1234L;
        final String host = "h1";
        final String ip = "127.0.0.1";
        final int port = 9300;
        final String poolName = "jvector";
        final int parallelism = 7;

        ThreadPool.Info fjInfo = new ThreadPool.Info(poolName, ThreadPool.ThreadPoolType.FORK_JOIN, parallelism, parallelism, null, null);
        ThreadPoolStats.Stats dummyStats = new ThreadPoolStats.Stats.Builder().name(poolName)
            .threads(0)
            .queue(0)
            .active(0)
            .rejected(0)
            .largest(0)
            .completed(0)
            .waitTimeNanos(-1)
            .parallelism(parallelism)
            .build();

        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());
        action.writeRow(table, nodeName, nodeId, eid, pid, host, ip, port, poolName, fjInfo, dummyStats);

        assertEquals(1, table.getRows().size());
        Map<String, Integer> idx = indexOf(table);
        List<Table.Cell> row = table.getRows().get(0);

        assertEquals(poolName, row.get(idx.get("name")).value);
        assertEquals("fork_join", row.get(idx.get("type")).value);
        assertEquals(0, row.get(idx.get("active")).value);
        assertEquals(0, row.get(idx.get("pool_size")).value);
        assertEquals(0, row.get(idx.get("queue")).value);
        assertEquals(-1, row.get(idx.get("queue_size")).value);
        assertEquals(0, row.get(idx.get("rejected")).value);
        assertEquals(0, row.get(idx.get("largest")).value);
        assertEquals(0, row.get(idx.get("completed")).value);
        assertEquals(-1, row.get(idx.get("total_wait_time")).value);
        assertNull(row.get(idx.get("core")).value);
        assertNull(row.get(idx.get("max")).value);
        assertNull(row.get(idx.get("size")).value);
        assertNull(row.get(idx.get("keep_alive")).value);
        assertEquals(parallelism, row.get(idx.get("parallelism")).value);
    }

    public void testNonForkJoinRowScaling() {
        final String nodeName = "n2";
        final String nodeId = "id2";
        final String eid = "e2";
        final Long pid = 5678L;
        final String host = "h2";
        final String ip = "127.0.0.2";
        final int port = 9400;
        final String poolName = "generic";

        ThreadPool.Info scalingInfo = new ThreadPool.Info(poolName, ThreadPool.ThreadPoolType.SCALING, 1, 4, null, null);
        ThreadPoolStats.Stats stats = new ThreadPoolStats.Stats.Builder().name(poolName)
            .threads(3)
            .queue(2)
            .active(1)
            .rejected(5L)
            .largest(3)
            .completed(10L)
            .waitTimeNanos(111L)
            .parallelism(-1)
            .build();

        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());
        action.writeRow(table, nodeName, nodeId, eid, pid, host, ip, port, poolName, scalingInfo, stats);

        assertEquals(1, table.getRows().size());
        Map<String, Integer> idx = indexOf(table);
        List<Table.Cell> row = table.getRows().get(0);

        assertEquals(poolName, row.get(idx.get("name")).value);
        assertEquals("scaling", row.get(idx.get("type")).value);
        assertEquals(1, row.get(idx.get("active")).value);
        assertEquals(3, row.get(idx.get("pool_size")).value);
        assertEquals(2, row.get(idx.get("queue")).value);
        assertEquals(5L, row.get(idx.get("rejected")).value);
        assertEquals(3, row.get(idx.get("largest")).value);
        assertEquals(10L, row.get(idx.get("completed")).value);
        assertEquals(stats.getWaitTime(), row.get(idx.get("total_wait_time")).value);
        assertEquals(1, row.get(idx.get("core")).value);
        assertEquals(4, row.get(idx.get("max")).value);
        assertNull(row.get(idx.get("parallelism")).value);
    }

    public void testForkJoinRowParallelismZero() {
        final String poolName = "fj_zero";
        final int parallelism = 0;
        ThreadPool.Info fjInfo = new ThreadPool.Info(poolName, ThreadPool.ThreadPoolType.FORK_JOIN, parallelism, parallelism, null, null);
        ThreadPoolStats.Stats dummyStats = new ThreadPoolStats.Stats.Builder().name(poolName)
            .threads(0)
            .queue(0)
            .active(0)
            .rejected(0)
            .largest(0)
            .completed(0)
            .waitTimeNanos(-1)
            .parallelism(parallelism)
            .build();

        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());
        action.writeRow(table, "n", "id", "eid", 1L, "h", "ip", 9300, poolName, fjInfo, dummyStats);
        assertEquals(parallelism, table.getRows().get(0).get(indexOf(table).get("parallelism")).value);
    }

    public void testForkJoinRowParallelismNegative() {
        final String poolName = "fj_negative";
        final int parallelism = -5;
        ThreadPool.Info fjInfo = new ThreadPool.Info(poolName, ThreadPool.ThreadPoolType.FORK_JOIN, parallelism, parallelism, null, null);
        ThreadPoolStats.Stats dummyStats = new ThreadPoolStats.Stats.Builder().name(poolName)
            .threads(0)
            .queue(0)
            .active(0)
            .rejected(0)
            .largest(0)
            .completed(0)
            .waitTimeNanos(-1)
            .parallelism(parallelism)
            .build();

        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());
        action.writeRow(table, "n", "id", "eid", 1L, "h", "ip", 9300, poolName, fjInfo, dummyStats);
        assertEquals(parallelism, table.getRows().get(0).get(indexOf(table).get("parallelism")).value);
    }

    public void testForkJoinRowNullInfo() {
        final String poolName = "fj_nullinfo";
        final int parallelism = 3;
        ThreadPool.Info fjInfo = null; // null info
        ThreadPoolStats.Stats dummyStats = new ThreadPoolStats.Stats.Builder().name(poolName)
            .threads(0)
            .queue(0)
            .active(0)
            .rejected(0)
            .largest(0)
            .completed(0)
            .waitTimeNanos(-1)
            .parallelism(parallelism)
            .build();

        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());
        action.writeRow(table, "n", "id", "eid", 1L, "h", "ip", 9300, poolName, fjInfo, dummyStats);

        // Assert that the row is still written, and 'parallelism' is null
        assertNull(table.getRows().get(0).get(indexOf(table).get("parallelism")).value);
    }

    public void testForkJoinRowNullStats() {
        final String poolName = "fj_nullstats";
        final int parallelism = 4;
        ThreadPool.Info fjInfo = new ThreadPool.Info(poolName, ThreadPool.ThreadPoolType.FORK_JOIN, parallelism, parallelism, null, null);
        ThreadPoolStats.Stats dummyStats = null; // null stats

        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());
        action.writeRow(table, "n", "id", "eid", 1L, "h", "ip", 9300, poolName, fjInfo, dummyStats);

        // All stat fields should be defaults (0, -1, or null), but parallelism should still be present
        assertEquals(parallelism, table.getRows().get(0).get(indexOf(table).get("parallelism")).value);
    }

    public void testMultipleForkJoinRows() {
        String[] poolNames = { "fj1", "fj2" };
        int[] parallelisms = { 3, 5 };
        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());

        for (int i = 0; i < poolNames.length; i++) {
            ThreadPool.Info fjInfo = new ThreadPool.Info(
                poolNames[i],
                ThreadPool.ThreadPoolType.FORK_JOIN,
                parallelisms[i],
                parallelisms[i],
                null,
                null
            );
            ThreadPoolStats.Stats dummyStats = new ThreadPoolStats.Stats.Builder().name(poolNames[i])
                .threads(0)
                .queue(0)
                .active(0)
                .rejected(0)
                .largest(0)
                .completed(0)
                .waitTimeNanos(-1)
                .parallelism(parallelisms[i])
                .build();
            action.writeRow(table, "n" + i, "id" + i, "eid" + i, 1L, "h" + i, "ip" + i, 9300 + i, poolNames[i], fjInfo, dummyStats);
        }
        assertEquals(2, table.getRows().size());
        Map<String, Integer> idx = indexOf(table);
        assertEquals(3, table.getRows().get(0).get(idx.get("parallelism")).value);
        assertEquals(5, table.getRows().get(1).get(idx.get("parallelism")).value);
    }

    public void testForkJoinRowLargeParallelism() {
        final String poolName = "fj_large";
        final int parallelism = Integer.MAX_VALUE;
        ThreadPool.Info fjInfo = new ThreadPool.Info(poolName, ThreadPool.ThreadPoolType.FORK_JOIN, parallelism, parallelism, null, null);
        ThreadPoolStats.Stats dummyStats = new ThreadPoolStats.Stats.Builder().name(poolName)
            .threads(0)
            .queue(0)
            .active(0)
            .rejected(0)
            .largest(0)
            .completed(0)
            .waitTimeNanos(-1)
            .parallelism(parallelism)
            .build();

        Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());
        action.writeRow(table, "n", "id", "eid", 1L, "h", "ip", 9300, poolName, fjInfo, dummyStats);

        assertEquals(parallelism, table.getRows().get(0).get(indexOf(table).get("parallelism")).value);
    }

    public void testTableHeadersAndAliases() {
        {
            RestThreadPoolAction action = new RestThreadPoolAction();
            Table table = action.getTableWithHeader(new FakeRestRequest.Builder(xContentRegistry()).build());

            // Expected headers in order (from the code)
            String[] expectedHeaders = new String[] {
                "node_name",
                "node_id",
                "ephemeral_node_id",
                "pid",
                "host",
                "ip",
                "port",
                "name",
                "type",
                "active",
                "pool_size",
                "queue",
                "queue_size",
                "rejected",
                "largest",
                "completed",
                "total_wait_time",
                "core",
                "max",
                "size",
                "keep_alive",
                "parallelism" };

            Map<String, String[]> expectedAliases = Map.ofEntries(
                Map.entry("node_name", new String[] { "nn" }),
                Map.entry("node_id", new String[] { "id" }),
                Map.entry("ephemeral_node_id", new String[] { "eid" }),
                Map.entry("pid", new String[] { "p" }),
                Map.entry("host", new String[] { "h" }),
                Map.entry("ip", new String[] { "i" }),
                Map.entry("port", new String[] { "po" }),
                Map.entry("name", new String[] { "n" }),
                Map.entry("type", new String[] { "t" }),
                Map.entry("active", new String[] { "a" }),
                Map.entry("pool_size", new String[] { "psz" }),
                Map.entry("queue", new String[] { "q" }),
                Map.entry("queue_size", new String[] { "qs" }),
                Map.entry("rejected", new String[] { "r" }),
                Map.entry("largest", new String[] { "l" }),
                Map.entry("completed", new String[] { "c" }),
                Map.entry("total_wait_time", new String[] { "twt" }),
                Map.entry("core", new String[] { "cr" }),
                Map.entry("max", new String[] { "mx" }),
                Map.entry("size", new String[] { "sz" }),
                Map.entry("keep_alive", new String[] { "ka" }),
                Map.entry("parallelism", new String[] { "pl" })
            );

            // Check header names and order
            List<Table.Cell> headers = table.getHeaders();
            assertEquals("Header count", expectedHeaders.length, headers.size());
            for (int i = 0; i < expectedHeaders.length; i++) {
                assertEquals("Header at " + i, expectedHeaders[i], headers.get(i).value.toString());
            }

            // Check aliases
            for (Table.Cell header : headers) {
                String name = header.value.toString();
                String[] aliases = expectedAliases.get(name);
                if (aliases != null) {
                    String aliasValue = header.attr.get("alias");
                    if (aliasValue != null) {
                        List<String> aliasList = Arrays.asList(aliasValue.split(","));
                        for (String alias : aliases) {
                            assertTrue("Alias " + alias + " for header " + name, aliasList.contains(alias));
                        }
                    } else {
                        fail("No alias found for header: " + name);
                    }
                }
            }
        }
    }

    public void testBuildTableWithForkJoinPool() throws Exception {
        // Arrange: Build minimal fake cluster state
        String nodeId = "node-1";
        String nodeName = "n1";
        String host = "localhost";
        String ip = "127.0.0.1";
        int port = 9300;

        // 1. Discovery node
        DiscoveryNode discoveryNode = new DiscoveryNode(
            nodeName,
            nodeId,
            new TransportAddress(InetAddress.getByName(ip), port),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        // 2. ClusterStateResponse
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(discoveryNode).build();
        ClusterState mockClusterState = mock(ClusterState.class);
        when(mockClusterState.nodes()).thenReturn(discoveryNodes);
        ClusterStateResponse clusterStateResponse = mock(ClusterStateResponse.class);
        when(clusterStateResponse.getState()).thenReturn(mockClusterState);

        // 3. ThreadPool.Info for ForkJoin
        String poolName = "jvector";
        int parallelism = 4;
        ThreadPool.Info fjInfo = new ThreadPool.Info(poolName, ThreadPool.ThreadPoolType.FORK_JOIN, parallelism, parallelism, null, null);

        // 4. NodeInfoResponse
        NodeInfo nodeInfo = mock(NodeInfo.class);
        ThreadPoolInfo threadPoolInfo = new ThreadPoolInfo(List.of(fjInfo));
        when(nodeInfo.getInfo(ThreadPoolInfo.class)).thenReturn(threadPoolInfo);
        ProcessInfo processInfo = mock(ProcessInfo.class);
        when(processInfo.getId()).thenReturn(1234L);
        when(nodeInfo.getInfo(ProcessInfo.class)).thenReturn(processInfo);
        when(nodeInfo.getInfo(ThreadPoolInfo.class)).thenReturn(threadPoolInfo);
        Map<String, NodeInfo> nodeInfoMap = Map.of(nodeId, nodeInfo);
        NodesInfoResponse nodesInfoResponse = mock(NodesInfoResponse.class);
        when(nodesInfoResponse.getNodesMap()).thenReturn(nodeInfoMap);

        // 5. ThreadPoolStats.Stats for ForkJoin
        ThreadPoolStats.Stats fjStats = new ThreadPoolStats.Stats.Builder().name(poolName)
            .threads(0)
            .queue(0)
            .active(0)
            .rejected(0)
            .largest(0)
            .completed(0)
            .waitTimeNanos(-1)
            .parallelism(parallelism)
            .build();
        ThreadPoolStats threadPoolStats = new ThreadPoolStats(new ArrayList<>(List.of(fjStats)));
        NodeStats nodeStats = mock(NodeStats.class);
        when(nodeStats.getThreadPool()).thenReturn(threadPoolStats);
        Map<String, NodeStats> nodeStatsMap = Map.of(nodeId, nodeStats);
        NodesStatsResponse nodesStatsResponse = mock(NodesStatsResponse.class);
        when(nodesStatsResponse.getNodes()).thenReturn(List.of(nodeStats));
        when(nodesStatsResponse.getNodesMap()).thenReturn(nodeStatsMap);

        // 6. Fake REST request
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();

        // Act: Call buildTable directly
        Table table = action.buildTable(request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse);

        // Assert
        List<Table.Cell> row = table.getRows().get(0);
        Map<String, Integer> idx = indexOf(table);

        assertEquals(poolName, row.get(idx.get("name")).value);
        assertEquals("fork_join", row.get(idx.get("type")).value);
        assertEquals(parallelism, row.get(idx.get("parallelism")).value);
        // Optionally assert other columns as well
    }

    public void testInvalidBooleanParam() {
        RestThreadPoolAction action = new RestThreadPoolAction();
        NodeClient client = mock(NodeClient.class);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(
            Collections.singletonMap("local", "notABoolean")
        ).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.doCatRequest(request, client));
        assertTrue(e.getMessage().contains("only [true] or [false] are allowed"));
    }

    public void testInvalidTimeoutParam() {
        RestThreadPoolAction action = new RestThreadPoolAction();
        NodeClient client = mock(NodeClient.class);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(
            Collections.singletonMap("cluster_manager_timeout", "notATime")
        ).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.doCatRequest(request, client));
        assertTrue(e.getMessage().contains("failed to parse setting [cluster_manager_timeout]"));
    }

    private static Map<String, Integer> indexOf(Table t) {
        Map<String, Integer> m = new HashMap<>();
        for (int i = 0; i < t.getHeaders().size(); i++) {
            m.put(t.getHeaders().get(i).value.toString(), i);
        }
        return m;
    }
}
