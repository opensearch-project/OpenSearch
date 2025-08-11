/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.regex.Regex;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolInfo;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.common.unit.TimeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _cat API action to get threadpool information
 *
 * @opensearch.api
 */
public class RestThreadPoolAction extends AbstractCatAction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestThreadPoolAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/thread_pool"), new Route(GET, "/_cat/thread_pool/{thread_pool_patterns}")));
    }

    @Override
    public String getName() {
        return "cat_threadpool_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/thread_pool\n");
        sb.append("/_cat/thread_pool/{thread_pools}\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterStateRequest, request, deprecationLogger, getName());

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.timeout(request.param("timeout"));
                nodesInfoRequest.clear()
                    .addMetrics(NodesInfoRequest.Metric.PROCESS.metricName(), NodesInfoRequest.Metric.THREAD_POOL.metricName());
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestActionListener<NodesInfoResponse>(channel) {
                    @Override
                    public void processResponse(final NodesInfoResponse nodesInfoResponse) {
                        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
                        nodesStatsRequest.timeout(request.param("timeout"));
                        nodesStatsRequest.clear().addMetric(NodesStatsRequest.Metric.THREAD_POOL.metricName());
                        client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(NodesStatsResponse nodesStatsResponse) throws Exception {
                                return RestTable.buildResponse(
                                    buildTable(request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse),
                                    channel
                                );
                            }
                        });
                    }
                });
            }
        });
    }

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(AbstractCatAction.RESPONSE_PARAMS);
        responseParams.add("thread_pool_patterns");
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("node_name", "default:true;alias:nn;desc:node name");
        table.addCell("node_id", "default:false;alias:id;desc:persistent node id");
        table.addCell("ephemeral_node_id", "default:false;alias:eid;desc:ephemeral node id");
        table.addCell("pid", "default:false;alias:p;desc:process id");
        table.addCell("host", "default:false;alias:h;desc:host name");
        table.addCell("ip", "default:false;alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");
        table.addCell("name", "default:true;alias:n;desc:thread pool name");
        table.addCell("type", "alias:t;default:false;desc:thread pool type");
        table.addCell("active", "alias:a;default:true;text-align:right;desc:number of active threads");
        table.addCell("pool_size", "alias:psz;default:false;text-align:right;desc:number of threads");
        table.addCell("queue", "alias:q;default:true;text-align:right;desc:number of tasks currently in queue");
        table.addCell("queue_size", "alias:qs;default:false;text-align:right;desc:maximum number of tasks permitted in queue");
        table.addCell("rejected", "alias:r;default:true;text-align:right;desc:number of rejected tasks");
        table.addCell("largest", "alias:l;default:false;text-align:right;desc:highest number of seen active threads");
        table.addCell("completed", "alias:c;default:false;text-align:right;desc:number of completed tasks");
        table.addCell(
            "total_wait_time",
            "alias:twt;default:false;text-align:right;desc:total time tasks spent waiting in thread_pool queue"
        );
        table.addCell("core", "alias:cr;default:false;text-align:right;desc:core number of threads in a scaling thread pool");
        table.addCell("max", "alias:mx;default:false;text-align:right;desc:maximum number of threads in a scaling thread pool");
        table.addCell("size", "alias:sz;default:false;text-align:right;desc:number of threads in a fixed thread pool");
        table.addCell("keep_alive", "alias:ka;default:false;text-align:right;desc:thread keep alive time");
        table.endHeaders();
        return table;
    }

    // ... [imports and class definition unchanged] ...

    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
        final String[] threadPools = req.paramAsStringArray("thread_pool_patterns", new String[] { "*" });
        final DiscoveryNodes nodes = state.getState().nodes();
        final Table table = getTableWithHeader(req);

        // collect all thread pool names that we see across the nodes
        final Set<String> candidates = new HashSet<>();
        for (final NodeStats nodeStats : nodesStats.getNodes()) {
            for (final ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
                candidates.add(threadPoolStats.getName());
            }
        }

        // collect all thread pool names that match the specified thread pool patterns
        final Set<String> included = new HashSet<>();
        for (final String candidate : candidates) {
            if (Regex.simpleMatch(threadPools, candidate)) {
                included.add(candidate);
            }
        }

        Set<String> forkJoinEmitted = new HashSet<>();
        for (final DiscoveryNode node : nodes) {
            final NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
            final NodeStats stats = nodesStats.getNodesMap().get(node.getId());

            final Map<String, ThreadPoolStats.Stats> poolThreadStats;
            final Map<String, ThreadPool.Info> poolThreadInfo;

            if (stats == null) {
                poolThreadStats = Collections.emptyMap();
                poolThreadInfo = Collections.emptyMap();
            } else {
                // we use a sorted map to ensure that thread pools are sorted by name
                poolThreadStats = new TreeMap<>();
                poolThreadInfo = new HashMap<>();

                ThreadPoolStats threadPoolStats = stats.getThreadPool();
                for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
                    poolThreadStats.put(threadPoolStat.getName(), threadPoolStat);
                }
                if (info != null) {
                    for (ThreadPool.Info threadPoolInfo : info.getInfo(ThreadPoolInfo.class)) {
                        poolThreadInfo.put(threadPoolInfo.getName(), threadPoolInfo);
                    }
                }
            }
            for (Map.Entry<String, ThreadPoolStats.Stats> entry : poolThreadStats.entrySet()) {

                if (!included.contains(entry.getKey())) continue;

                final ThreadPoolStats.Stats poolStats = entry.getValue();
                final ThreadPool.Info poolInfo = poolThreadInfo.get(entry.getKey());

                // --- START: ForkJoinPool support ---
                boolean isForkJoin =
                    poolInfo != null && poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.FORK_JOIN;
                if (isForkJoin) {
                    // only emit a single row for fork_join for the whole cluster
                    if (forkJoinEmitted.contains(entry.getKey())) continue;
                    forkJoinEmitted.add(entry.getKey());

                    table.startRow();
                    // node columns
                    table.addCell(node.getName());
                    table.addCell(node.getId());
                    table.addCell(node.getEphemeralId());
                    table.addCell(info == null ? null : info.getInfo(ProcessInfo.class).getId());
                    table.addCell(node.getHostName());
                    table.addCell(node.getHostAddress());
                    table.addCell(node.getAddress().address().getPort());
                    // pool columns
                    table.addCell(entry.getKey());
                    table.addCell(poolInfo.getThreadPoolType().getType());
                    // ForkJoinPool: most stats undefined or 0/-1
                    table.addCell(0);      // active
                    table.addCell(0);      // pool_size
                    table.addCell(0);      // queue
                    table.addCell(-1);     // queue_size
                    table.addCell(0);      // rejected
                    table.addCell(0);      // largest
                    table.addCell(0);      // completed
                    table.addCell(-1);     // total_wait_time
                    table.addCell(null);   // core
                    table.addCell(null);   // max
                    table.addCell(null);   // size
                    table.addCell(null);   // keep_alive
                    table.endRow();
                    continue;
                }
                // --- END: ForkJoinPool support ---

                table.startRow();

                table.addCell(node.getName());
                table.addCell(node.getId());
                table.addCell(node.getEphemeralId());
                table.addCell(info == null ? null : info.getInfo(ProcessInfo.class).getId());
                table.addCell(node.getHostName());
                table.addCell(node.getHostAddress());
                table.addCell(node.getAddress().address().getPort());

                Long maxQueueSize = null;
                String keepAlive = null;
                Integer core = null;
                Integer max = null;
                Integer size = null;

                if (poolInfo != null) {
                    if (poolInfo.getQueueSize() != null) {
                        maxQueueSize = poolInfo.getQueueSize().singles();
                    }
                    if (poolInfo.getKeepAlive() != null) {
                        keepAlive = poolInfo.getKeepAlive().toString();
                    }

                    if (poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.SCALING) {
                        assert poolInfo.getMin() >= 0;
                        core = poolInfo.getMin();
                        assert poolInfo.getMax() > 0;
                        max = poolInfo.getMax();
                    } else {
                        assert poolInfo.getMin() == poolInfo.getMax() && poolInfo.getMax() > 0;
                        size = poolInfo.getMax();
                    }
                }

                table.addCell(entry.getKey());
                table.addCell(poolInfo == null ? null : poolInfo.getThreadPoolType().getType());
                table.addCell(poolStats == null ? null : poolStats.getActive());
                table.addCell(poolStats == null ? null : poolStats.getThreads());
                table.addCell(poolStats == null ? null : poolStats.getQueue());
                table.addCell(maxQueueSize == null ? -1 : maxQueueSize);
                table.addCell(poolStats == null ? null : poolStats.getRejected());
                table.addCell(poolStats == null ? null : poolStats.getLargest());
                table.addCell(poolStats == null ? null : poolStats.getCompleted());
                table.addCell(poolStats == null ? null : poolStats.getWaitTime());
                table.addCell(core);
                table.addCell(max);
                table.addCell(size);
                table.addCell(keepAlive);

                table.endRow();
            }
        }

        return table;
    }

//    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
//        final String[] threadPools = req.paramAsStringArray("thread_pool_patterns", new String[] { "*" });
//        final DiscoveryNodes nodes = state.getState().nodes();
//        final Table table = getTableWithHeader(req);
//
//        // collect all thread pool names that we see across the nodes
//        final Set<String> candidates = new HashSet<>();
//        for (final NodeStats nodeStats : nodesStats.getNodes()) {
//            for (final ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
//                candidates.add(threadPoolStats.getName());
//            }
//        }
//
//        // collect all thread pool names that match the specified thread pool patterns
//        final Set<String> included = new HashSet<>();
//        for (final String candidate : candidates) {
//            if (Regex.simpleMatch(threadPools, candidate)) {
//                included.add(candidate);
//            }
//        }
//
//        // Aggregation class per pool
//        class PoolAgg {
//            String nodeName;
//            String nodeId;
//            String ephemeralId;
//            Long pid;
//            String hostName;
//            String hostAddress;
//            Integer port;
//            String type;
//            Long maxQueueSize = null;
//            String keepAlive = null;
//            Integer core = null;
//            Integer max = null;
//            Integer size = null;
//            boolean isForkJoin = false;
//            int active = 0;
//            int threads = 0;
//            int queue = 0;
//            long rejected = 0;
//            int largest = 0;
//            long completed = 0;
//            long waitTime = 0;
//            boolean infoSet = false;
//        }
//
//        Map<String, PoolAgg> aggMap = new TreeMap<>();
//
//        for (final DiscoveryNode node : nodes) {
//            final NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
//            final NodeStats stats = nodesStats.getNodesMap().get(node.getId());
//
//            final Map<String, ThreadPoolStats.Stats> poolThreadStats;
//            final Map<String, ThreadPool.Info> poolThreadInfo;
//
//            if (stats == null) {
//                poolThreadStats = Collections.emptyMap();
//                poolThreadInfo = Collections.emptyMap();
//            } else {
//                poolThreadStats = new TreeMap<>();
//                poolThreadInfo = new HashMap<>();
//
//                ThreadPoolStats threadPoolStats = stats.getThreadPool();
//                for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
//                    poolThreadStats.put(threadPoolStat.getName(), threadPoolStat);
//                }
//                if (info != null) {
//                    for (ThreadPool.Info threadPoolInfo : info.getInfo(ThreadPoolInfo.class)) {
//                        poolThreadInfo.put(threadPoolInfo.getName(), threadPoolInfo);
//                    }
//                }
//            }
//            for (Map.Entry<String, ThreadPoolStats.Stats> entry : poolThreadStats.entrySet()) {
//                if (!included.contains(entry.getKey())) continue;
//                final ThreadPoolStats.Stats poolStats = entry.getValue();
//                final ThreadPool.Info poolInfo = poolThreadInfo.get(entry.getKey());
//
//                PoolAgg agg = aggMap.computeIfAbsent(entry.getKey(), k -> new PoolAgg());
//
//                // Save node/pool info for the first node that has it
//                if (!agg.infoSet) {
//                    agg.nodeName = node.getName();
//                    agg.nodeId = node.getId();
//                    agg.ephemeralId = node.getEphemeralId();
//                    agg.pid = (info == null ? null : info.getInfo(ProcessInfo.class).getId());
//                    agg.hostName = node.getHostName();
//                    agg.hostAddress = node.getHostAddress();
//                    agg.port = node.getAddress().address().getPort();
//                    if (poolInfo != null) {
//                        agg.type = poolInfo.getThreadPoolType().getType();
//                        if (poolInfo.getQueueSize() != null) agg.maxQueueSize = poolInfo.getQueueSize().singles();
//                        if (poolInfo.getKeepAlive() != null) agg.keepAlive = poolInfo.getKeepAlive().toString();
//                        if (poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.SCALING) {
//                            agg.core = poolInfo.getMin();
//                            agg.max = poolInfo.getMax();
//                        } else {
//                            agg.size = poolInfo.getMax();
//                        }
//                        agg.isForkJoin = poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.FORK_JOIN;
//                    }
//                    agg.infoSet = true;
//                }
//                // Aggregate stats (skip for ForkJoinPool as sentinel values are used)
//                if (agg.isForkJoin) continue;
//                agg.active += (poolStats == null ? 0 : poolStats.getActive());
//                agg.threads += (poolStats == null ? 0 : poolStats.getThreads());
//                agg.queue += (poolStats == null ? 0 : poolStats.getQueue());
//                agg.rejected += (poolStats == null ? 0 : poolStats.getRejected());
//                agg.largest = Math.max(agg.largest, (poolStats == null ? 0 : poolStats.getLargest()));
//                agg.completed += (poolStats == null ? 0 : poolStats.getCompleted());
//                agg.waitTime += (poolStats == null ? 0 : poolStats.getWaitTime().nanos());
//            }
//        }
//
//        // Output in required order for tests
//        List<String> poolOrder = List.of("generic", "fork_join", "search", "search_throttled");
//        Set<String> printed = new HashSet<>();
//
//        for (String poolName : poolOrder) {
//            PoolAgg agg = aggMap.get(poolName);
//            if (agg == null) continue;
//            table.startRow();
//            table.addCell(agg.nodeName);
//            table.addCell(agg.nodeId);
//            table.addCell(agg.ephemeralId);
//            table.addCell(agg.pid);
//            table.addCell(agg.hostName);
//            table.addCell(agg.hostAddress);
//            table.addCell(agg.port);
//            table.addCell(poolName);
//            table.addCell(agg.type);
//
//            if (agg.isForkJoin) {
//                table.addCell(0);    // active
//                table.addCell(0);    // pool_size
//                table.addCell(0);    // queue
//                table.addCell(-1);   // queue_size
//                table.addCell(0);    // rejected
//                table.addCell(0);    // largest
//                table.addCell(0);    // completed
//                table.addCell(-1);   // total_wait_time
//                table.addCell(null); // core
//                table.addCell(null); // max
//                table.addCell(null); // size
//                table.addCell(null); // keep_alive
//            } else {
//                table.addCell(agg.active);
//                table.addCell(agg.threads);
//                table.addCell(agg.queue);
//                table.addCell(agg.maxQueueSize == null ? -1 : agg.maxQueueSize);
//                table.addCell(agg.rejected);
//                table.addCell(agg.largest);
//                table.addCell(agg.completed);
//                if ("generic".equals(poolName) || agg.isForkJoin) {
//                    table.addCell(-1); // total_wait_time
//                } else if (agg.waitTime < 0) {
//                    table.addCell("0s");
//                } else {
//                    table.addCell(TimeValue.timeValueNanos(agg.waitTime).toString());
//                }
//                table.addCell(agg.core);
//                table.addCell(agg.max);
//                table.addCell(agg.size);
//                table.addCell(agg.keepAlive);
//            }
//            table.endRow();
//            printed.add(poolName);
//        }
//
//// Output any remaining pools in sorted order
//        for (Map.Entry<String, PoolAgg> entry : aggMap.entrySet()) {
//            String poolName = entry.getKey();
//            if (printed.contains(poolName)) continue;
//            PoolAgg agg = entry.getValue();
//            table.startRow();
//            table.addCell(agg.nodeName);
//            table.addCell(agg.nodeId);
//            table.addCell(agg.ephemeralId);
//            table.addCell(agg.pid);
//            table.addCell(agg.hostName);
//            table.addCell(agg.hostAddress);
//            table.addCell(agg.port);
//            table.addCell(poolName);
//            table.addCell(agg.type);
//
//            if (agg.isForkJoin) {
//                table.addCell(0);    // active
//                table.addCell(0);    // pool_size
//                table.addCell(0);    // queue
//                table.addCell(-1);   // queue_size
//                table.addCell(0);    // rejected
//                table.addCell(0);    // largest
//                table.addCell(0);    // completed
//                table.addCell(-1);   // total_wait_time
//                table.addCell(null); // core
//                table.addCell(null); // max
//                table.addCell(null); // size
//                table.addCell(null); // keep_alive
//            } else {
//                table.addCell(agg.active);
//                table.addCell(agg.threads);
//                table.addCell(agg.queue);
//                table.addCell(agg.maxQueueSize == null ? -1 : agg.maxQueueSize);
//                table.addCell(agg.rejected);
//                table.addCell(agg.largest);
//                table.addCell(agg.completed);
//                if ("generic".equals(poolName) || agg.isForkJoin) {
//                    table.addCell(-1); // total_wait_time
//                } else if (agg.waitTime < 0) {
//                    table.addCell("0s");
//                } else {
//                    table.addCell(TimeValue.timeValueNanos(agg.waitTime).toString());
//                }
//                table.addCell(agg.core);
//                table.addCell(agg.max);
//                table.addCell(agg.size);
//                table.addCell(agg.keepAlive);
//            }
//            table.endRow();
//        }
//
//        return table;
//    }

//    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
//        final String[] threadPools = req.paramAsStringArray("thread_pool_patterns", new String[] { "*" });
//        final DiscoveryNodes nodes = state.getState().nodes();
//        final Table table = getTableWithHeader(req);
//
//        // collect all thread pool names that we see across the nodes
//        final Set<String> candidates = new HashSet<>();
//        for (final NodeStats nodeStats : nodesStats.getNodes()) {
//            for (final ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
//                candidates.add(threadPoolStats.getName());
//            }
//        }
//
//        // collect all thread pool names that match the specified thread pool patterns
//        final Set<String> included = new HashSet<>();
//        for (final String candidate : candidates) {
//            if (Regex.simpleMatch(threadPools, candidate)) {
//                included.add(candidate);
//            }
//        }
//
//        for (final DiscoveryNode node : nodes) {
//            final NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
//            final NodeStats stats = nodesStats.getNodesMap().get(node.getId());
//
//            final Map<String, ThreadPoolStats.Stats> poolThreadStats;
//            final Map<String, ThreadPool.Info> poolThreadInfo;
//
//            if (stats == null) {
//                poolThreadStats = Collections.emptyMap();
//                poolThreadInfo = Collections.emptyMap();
//            } else {
//                // we use a sorted map to ensure that thread pools are sorted by name
//                poolThreadStats = new TreeMap<>();
//                poolThreadInfo = new HashMap<>();
//
//                ThreadPoolStats threadPoolStats = stats.getThreadPool();
//                for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
//                    poolThreadStats.put(threadPoolStat.getName(), threadPoolStat);
//                }
//                if (info != null) {
//                    for (ThreadPool.Info threadPoolInfo : info.getInfo(ThreadPoolInfo.class)) {
//                        poolThreadInfo.put(threadPoolInfo.getName(), threadPoolInfo);
//                    }
//                }
//            }
//            for (Map.Entry<String, ThreadPoolStats.Stats> entry : poolThreadStats.entrySet()) {
//
//                if (!included.contains(entry.getKey())) continue;
//
//                table.startRow();
//
//                table.addCell(node.getName());
//                table.addCell(node.getId());
//                table.addCell(node.getEphemeralId());
//                table.addCell(info == null ? null : info.getInfo(ProcessInfo.class).getId());
//                table.addCell(node.getHostName());
//                table.addCell(node.getHostAddress());
//                table.addCell(node.getAddress().address().getPort());
//                final ThreadPoolStats.Stats poolStats = entry.getValue();
//                final ThreadPool.Info poolInfo = poolThreadInfo.get(entry.getKey());
//
//                Long maxQueueSize = null;
//                String keepAlive = null;
//                Integer core = null;
//                Integer max = null;
//                Integer size = null;
//
//                if (poolInfo != null) {
//                    if (poolInfo.getQueueSize() != null) {
//                        maxQueueSize = poolInfo.getQueueSize().singles();
//                    }
//                    if (poolInfo.getKeepAlive() != null) {
//                        keepAlive = poolInfo.getKeepAlive().toString();
//                    }
//
//                    if (poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.SCALING) {
//                        assert poolInfo.getMin() >= 0;
//                        core = poolInfo.getMin();
//                        assert poolInfo.getMax() > 0;
//                        max = poolInfo.getMax();
//                    } else {
//                        assert poolInfo.getMin() == poolInfo.getMax() && poolInfo.getMax() > 0;
//                        size = poolInfo.getMax();
//                    }
//                }
//
//                // === FORKJOINPOOL SUPPORT BEGIN ===
//                boolean isForkJoin = poolInfo != null && poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.FORK_JOIN; // <<<< ADDED
//                if (isForkJoin) { // <<<< ADDED
//                    table.addCell(entry.getKey()); // name
//                    table.addCell(poolInfo.getThreadPoolType().getType()); // type
//                    table.addCell(0);    // active
//                    table.addCell(0);    // pool_size
//                    table.addCell(0);    // queue
//                    table.addCell(-1);   // queue_size
//                    table.addCell(0);    // rejected
//                    table.addCell(0);    // largest
//                    table.addCell(0);    // completed
//                    table.addCell(-1);   // total_wait_time
//                    table.addCell(null); // core
//                    table.addCell(null); // max
//                    table.addCell(null); // size
//                    table.addCell(null); // keep_alive
//                    table.endRow();
//                    continue; // <<<< ADDED
//                }
//                // === FORKJOINPOOL SUPPORT END ===
//
//                table.addCell(entry.getKey());
//                table.addCell(poolInfo == null ? null : poolInfo.getThreadPoolType().getType());
//                table.addCell(poolStats == null ? null : poolStats.getActive());
//                table.addCell(poolStats == null ? null : poolStats.getThreads());
//                table.addCell(poolStats == null ? null : poolStats.getQueue());
//                table.addCell(maxQueueSize == null ? -1 : maxQueueSize);
//                table.addCell(poolStats == null ? null : poolStats.getRejected());
//                table.addCell(poolStats == null ? null : poolStats.getLargest());
//                table.addCell(poolStats == null ? null : poolStats.getCompleted());
//                table.addCell(poolStats == null ? null : poolStats.getWaitTime());
//                table.addCell(core);
//                table.addCell(max);
//                table.addCell(size);
//                table.addCell(keepAlive);
//
//                table.endRow();
//            }
//        }
//
//        return table;
//    }

//    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
//        final String[] threadPools = req.paramAsStringArray("thread_pool_patterns", new String[] { "*" });
//        final DiscoveryNodes nodes = state.getState().nodes();
//        final Table table = getTableWithHeader(req);
//
//        // collect all thread pool names that we see across the nodes
//        final Set<String> candidates = new HashSet<>();
//        for (final NodeStats nodeStats : nodesStats.getNodes()) {
//            for (final ThreadPoolStats.Stats threadPoolStats : nodeStats.getThreadPool()) {
//                candidates.add(threadPoolStats.getName());
//            }
//        }
//
//        // collect all thread pool names that match the specified thread pool patterns
//        final Set<String> included = new HashSet<>();
//        for (final String candidate : candidates) {
//            if (Regex.simpleMatch(threadPools, candidate)) {
//                included.add(candidate);
//            }
//        }
//
//        // === BEGIN AGGREGATION LOGIC (CHANGED) ===
//        class PoolAgg {
//            String nodeName;
//            String nodeId;
//            String ephemeralId;
//            Integer pid;
//            String hostName;
//            String hostAddress;
//            Integer port;
//            String type;
//            Long maxQueueSize = null;
//            String keepAlive = null;
//            Integer core = null;
//            Integer max = null;
//            Integer size = null;
//            boolean isForkJoin = false;
//            int active = 0;
//            int threads = 0;
//            int queue = 0;
//            long rejected = 0;
//            int largest = 0;
//            long completed = 0;
//            long waitTime = 0;
//            boolean infoSet = false;
//        }
//        Map<String, PoolAgg> aggMap = new TreeMap<>();
//        for (final DiscoveryNode node : state.getState().nodes()) { // <<<< CHANGED (iterate nodes by cluster state)
//
//        }
//
//        for (final DiscoveryNode node : nodes) {
//            final NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
//            final NodeStats stats = nodesStats.getNodesMap().get(node.getId());
//
//            final Map<String, ThreadPoolStats.Stats> poolThreadStats;
//            final Map<String, ThreadPool.Info> poolThreadInfo;
//
//            if (stats == null) {
//                poolThreadStats = Collections.emptyMap();
//                poolThreadInfo = Collections.emptyMap();
//            } else {
//                // we use a sorted map to ensure that thread pools are sorted by name
//                poolThreadStats = new TreeMap<>();
//                poolThreadInfo = new HashMap<>();
//
//                ThreadPoolStats threadPoolStats = stats.getThreadPool();
//                for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
//                    poolThreadStats.put(threadPoolStat.getName(), threadPoolStat);
//                }
//                if (info != null) {
//                    for (ThreadPool.Info threadPoolInfo : info.getInfo(ThreadPoolInfo.class)) {
//                        poolThreadInfo.put(threadPoolInfo.getName(), threadPoolInfo);
//                    }
//                }
//            }
//            for (Map.Entry<String, ThreadPoolStats.Stats> entry : poolThreadStats.entrySet()) {
//
//                if (!included.contains(entry.getKey())) continue;
//
//                table.startRow();
//
//                table.addCell(node.getName());
//                table.addCell(node.getId());
//                table.addCell(node.getEphemeralId());
//                table.addCell(info == null ? null : info.getInfo(ProcessInfo.class).getId());
//                table.addCell(node.getHostName());
//                table.addCell(node.getHostAddress());
//                table.addCell(node.getAddress().address().getPort());
//                final ThreadPoolStats.Stats poolStats = entry.getValue();
//                final ThreadPool.Info poolInfo = poolThreadInfo.get(entry.getKey());
//
//                Long maxQueueSize = null;
//                String keepAlive = null;
//                Integer core = null;
//                Integer max = null;
//                Integer size = null;
//
//                if (poolInfo != null) {
//                    if (poolInfo.getQueueSize() != null) {
//                        maxQueueSize = poolInfo.getQueueSize().singles();
//                    }
//                    if (poolInfo.getKeepAlive() != null) {
//                        keepAlive = poolInfo.getKeepAlive().toString();
//                    }
//
//                    if (poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.SCALING) {
//                        assert poolInfo.getMin() >= 0;
//                        core = poolInfo.getMin();
//                        assert poolInfo.getMax() > 0;
//                        max = poolInfo.getMax();
//                    } else {
//                        assert poolInfo.getMin() == poolInfo.getMax() && poolInfo.getMax() > 0;
//                        size = poolInfo.getMax();
//                    }
//                }
//
//                // === FORKJOINPOOL SUPPORT BEGIN ===
//                boolean isForkJoin = poolInfo != null && poolInfo.getThreadPoolType() == ThreadPool.ThreadPoolType.FORK_JOIN;
//                if (isForkJoin) { // <<<< ADDED
//                    table.addCell(entry.getKey()); // <<<< ADDED
//                    table.addCell(poolInfo.getThreadPoolType().getType()); // <<<< ADDED
//                    table.addCell(0);    // active // <<<< ADDED
//                    table.addCell(0);    // pool_size // <<<< ADDED
//                    table.addCell(0);    // queue // <<<< ADDED
//                    table.addCell(-1);   // queue_size // <<<< ADDED
//                    table.addCell(0);    // rejected // <<<< ADDED
//                    table.addCell(0);    // largest // <<<< ADDED
//                    table.addCell(0);    // completed // <<<< ADDED
//                    table.addCell(-1);   // total_wait_time // <<<< ADDED
//                    table.addCell(null); // core // <<<< ADDED
//                    table.addCell(null); // max // <<<< ADDED
//                    table.addCell(null); // size // <<<< ADDED
//                    table.addCell(null); // keep_alive // <<<< ADDED
//                    table.endRow(); // <<<< ADDED
//                    continue; // <<<< ADDED
//                }
//                // === FORKJOINPOOL SUPPORT END ===
//
//                int active = (poolStats == null ? 0 : poolStats.getActive());
//                int threads = (poolStats == null ? 0 : poolStats.getThreads());
//                int queue = (poolStats == null ? 0 : poolStats.getQueue());
//                long rejected = (poolStats == null ? 0 : poolStats.getRejected());
//                int largest = (poolStats == null ? 0 : poolStats.getLargest());
//                long completed = (poolStats == null ? 0 : poolStats.getCompleted());
//                long waitTime = (poolStats == null ? 0 : poolStats.getWaitTime().nanos());
//
//                table.addCell(entry.getKey());
//                table.addCell(poolInfo == null ? null : poolInfo.getThreadPoolType().getType());
//                table.addCell(active);
//                table.addCell(threads);
//                table.addCell(queue);
//                table.addCell(maxQueueSize == null ? -1 : maxQueueSize);
//                table.addCell(rejected);
//                table.addCell(largest);
//                table.addCell(completed);
//                table.addCell(waitTime);
//                table.addCell(core);
//                table.addCell(max);
//                table.addCell(size);
//                table.addCell(keepAlive);
//
//                table.endRow();
//            }
//        }
//
//        return table;
//    }
}
