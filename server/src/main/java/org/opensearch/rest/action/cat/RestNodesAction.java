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
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.http.HttpInfo;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.get.GetStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.script.ScriptStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _cat API action to get node information
 *
 * @opensearch.api
 */
public class RestNodesAction extends AbstractCatAction {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestNodesAction.class);
    static final String LOCAL_DEPRECATED_MESSAGE = "Deprecated parameter [local] used. This parameter does not cause this API to act "
        + "locally, and should not be used. It will be unsupported in version 8.0.";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/nodes"));
    }

    @Override
    public String getName() {
        return "cat_nodes_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/nodes\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        if (request.hasParam("local")) {
            deprecationLogger.deprecate("cat_nodes_local_parameter", LOCAL_DEPRECATED_MESSAGE);
        }
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterStateRequest, request, deprecationLogger, getName());
        final boolean fullId = request.paramAsBoolean("full_id", false);
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.timeout(request.param("timeout"));
                nodesInfoRequest.clear()
                    .addMetrics(
                        NodesInfoRequest.Metric.JVM.metricName(),
                        NodesInfoRequest.Metric.OS.metricName(),
                        NodesInfoRequest.Metric.PROCESS.metricName(),
                        NodesInfoRequest.Metric.HTTP.metricName()
                    );
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestActionListener<NodesInfoResponse>(channel) {
                    @Override
                    public void processResponse(final NodesInfoResponse nodesInfoResponse) {
                        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
                        nodesStatsRequest.timeout(request.param("timeout"));
                        nodesStatsRequest.clear()
                            .indices(true)
                            .addMetrics(
                                NodesStatsRequest.Metric.JVM.metricName(),
                                NodesStatsRequest.Metric.OS.metricName(),
                                NodesStatsRequest.Metric.FS.metricName(),
                                NodesStatsRequest.Metric.PROCESS.metricName(),
                                NodesStatsRequest.Metric.SCRIPT.metricName()
                            );
                        nodesStatsRequest.indices().setIncludeIndicesStatsByLevel(true);
                        client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(NodesStatsResponse nodesStatsResponse) throws Exception {
                                return RestTable.buildResponse(
                                    buildTable(fullId, request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse),
                                    channel
                                );
                            }
                        });
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("id", "default:false;alias:id,nodeId;desc:unique node id");
        table.addCell("pid", "default:false;alias:p;desc:process id");
        table.addCell("ip", "alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");
        table.addCell("http_address", "default:false;alias:http;desc:bound http address");

        table.addCell("version", "default:false;alias:v;desc:os version");
        table.addCell("type", "default:false;alias:t;desc:os distribution type");
        table.addCell("build", "default:false;alias:b;desc:os build hash");
        table.addCell("jdk", "default:false;alias:j;desc:jdk version");
        table.addCell("disk.total", "default:false;alias:dt,diskTotal;text-align:right;desc:total disk space");
        table.addCell("disk.used", "default:false;alias:du,diskUsed;text-align:right;desc:used disk space");
        table.addCell("disk.avail", "default:false;alias:d,da,disk,diskAvail;text-align:right;desc:available disk space");
        table.addCell("disk.used_percent", "default:false;alias:dup,diskUsedPercent;text-align:right;desc:used disk space percentage");
        table.addCell("heap.current", "default:false;alias:hc,heapCurrent;text-align:right;desc:used heap");
        table.addCell("heap.percent", "alias:hp,heapPercent;text-align:right;desc:used heap ratio");
        table.addCell("heap.max", "default:false;alias:hm,heapMax;text-align:right;desc:max configured heap");
        table.addCell("ram.current", "default:false;alias:rc,ramCurrent;text-align:right;desc:used machine memory");
        table.addCell("ram.percent", "alias:rp,ramPercent;text-align:right;desc:used machine memory ratio");
        table.addCell("ram.max", "default:false;alias:rm,ramMax;text-align:right;desc:total machine memory");
        table.addCell("file_desc.current", "default:false;alias:fdc,fileDescriptorCurrent;text-align:right;desc:used file descriptors");
        table.addCell(
            "file_desc.percent",
            "default:false;alias:fdp,fileDescriptorPercent;text-align:right;desc:used file descriptor ratio"
        );
        table.addCell("file_desc.max", "default:false;alias:fdm,fileDescriptorMax;text-align:right;desc:max file descriptors");

        table.addCell("cpu", "alias:cpu;text-align:right;desc:recent cpu usage");
        table.addCell("load_1m", "alias:l;text-align:right;desc:1m load avg");
        table.addCell("load_5m", "alias:l;text-align:right;desc:5m load avg");
        table.addCell("load_15m", "alias:l;text-align:right;desc:15m load avg");
        table.addCell("uptime", "default:false;alias:u;text-align:right;desc:node uptime");
        // TODO: Deprecate "node.role", use "node.roles" which shows full node role names
        table.addCell(
            "node.role",
            "alias:r,role,nodeRole;desc:m:master eligible node, d:data node, i:ingest node, -:coordinating node only"
        );
        table.addCell("node.roles", "alias:rs,all roles;desc: -:coordinating node only");
        // TODO: Remove the header alias 'master', after removing MASTER_ROLE. It's added for compatibility when using parameter 'h=master'.
        table.addCell("cluster_manager", "alias:cm,m,master;desc:*:current cluster manager");
        table.addCell("name", "alias:n;desc:node name");

        table.addCell("completion.size", "alias:cs,completionSize;default:false;text-align:right;desc:size of completion");

        table.addCell("fielddata.memory_size", "alias:fm,fielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("fielddata.evictions", "alias:fe,fielddataEvictions;default:false;text-align:right;desc:fielddata evictions");

        table.addCell("query_cache.memory_size", "alias:qcm,queryCacheMemory;default:false;text-align:right;desc:used query cache");
        table.addCell("query_cache.evictions", "alias:qce,queryCacheEvictions;default:false;text-align:right;desc:query cache evictions");
        table.addCell("query_cache.hit_count", "alias:qchc,queryCacheHitCount;default:false;text-align:right;desc:query cache hit counts");
        table.addCell(
            "query_cache.miss_count",
            "alias:qcmc,queryCacheMissCount;default:false;text-align:right;desc:query cache miss counts"
        );

        table.addCell("request_cache.memory_size", "alias:rcm,requestCacheMemory;default:false;text-align:right;desc:used request cache");
        table.addCell(
            "request_cache.evictions",
            "alias:rce,requestCacheEvictions;default:false;text-align:right;desc:request cache evictions"
        );
        table.addCell(
            "request_cache.hit_count",
            "alias:rchc,requestCacheHitCount;default:false;text-align:right;desc:request cache hit counts"
        );
        table.addCell(
            "request_cache.miss_count",
            "alias:rcmc,requestCacheMissCount;default:false;text-align:right;desc:request cache miss counts"
        );

        table.addCell("flush.total", "alias:ft,flushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("flush.total_time", "alias:ftt,flushTotalTime;default:false;text-align:right;desc:time spent in flush");

        table.addCell("get.current", "alias:gc,getCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("get.time", "alias:gti,getTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("get.total", "alias:gto,getTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("get.exists_time", "alias:geti,getExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("get.exists_total", "alias:geto,getExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("get.missing_time", "alias:gmti,getMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("get.missing_total", "alias:gmto,getMissingTotal;default:false;text-align:right;desc:number of failed gets");

        table.addCell(
            "indexing.delete_current",
            "alias:idc,indexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions"
        );
        table.addCell("indexing.delete_time", "alias:idti,indexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("indexing.delete_total", "alias:idto,indexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell(
            "indexing.index_current",
            "alias:iic,indexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops"
        );
        table.addCell("indexing.index_time", "alias:iiti,indexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("indexing.index_total", "alias:iito,indexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");
        table.addCell(
            "indexing.index_failed",
            "alias:iif,indexingIndexFailed;default:false;text-align:right;desc:number of failed indexing ops"
        );

        table.addCell("merges.current", "alias:mc,mergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell(
            "merges.current_docs",
            "alias:mcd,mergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs"
        );
        table.addCell("merges.current_size", "alias:mcs,mergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("merges.total", "alias:mt,mergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("merges.total_docs", "alias:mtd,mergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("merges.total_size", "alias:mts,mergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("merges.total_time", "alias:mtt,mergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("refresh.total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("refresh.time", "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in refreshes");
        table.addCell("refresh.external_total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total external refreshes");
        table.addCell(
            "refresh.external_time",
            "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in external refreshes"
        );
        table.addCell(
            "refresh.listeners",
            "alias:rli,refreshListeners;default:false;text-align:right;" + "desc:number of pending refresh listeners"
        );

        table.addCell("script.compilations", "alias:scrcc,scriptCompilations;default:false;text-align:right;desc:script compilations");
        table.addCell(
            "script.cache_evictions",
            "alias:scrce,scriptCacheEvictions;default:false;text-align:right;desc:script cache evictions"
        );
        table.addCell(
            "script.compilation_limit_triggered",
            "alias:scrclt,scriptCacheCompilationLimitTriggered;default:false;"
                + "text-align:right;desc:script cache compilation limit triggered"
        );

        table.addCell("search.fetch_current", "alias:sfc,searchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("search.fetch_time", "alias:sfti,searchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("search.fetch_total", "alias:sfto,searchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("search.open_contexts", "alias:so,searchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("search.query_current", "alias:sqc,searchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("search.query_time", "alias:sqti,searchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("search.query_total", "alias:sqto,searchQueryTotal;default:false;text-align:right;desc:total query phase ops");
        table.addCell(
            "search.concurrent_query_current",
            "alias:scqc,searchConcurrentQueryCurrent;default:false;text-align:right;desc:current concurrent query phase ops"
        );
        table.addCell(
            "search.concurrent_query_time",
            "alias:scqti,searchConcurrentQueryTime;default:false;text-align:right;desc:time spent in concurrent query phase"
        );
        table.addCell(
            "search.concurrent_query_total",
            "alias:scqto,searchConcurrentQueryTotal;default:false;text-align:right;desc:total concurrent query phase ops"
        );
        table.addCell(
            "search.concurrent_avg_slice_count",
            "alias:casc,searchConcurrentAvgSliceCount;default:false;text-align:right;desc:average query concurrency"
        );
        table.addCell("search.scroll_current", "alias:scc,searchScrollCurrent;default:false;text-align:right;desc:open scroll contexts");
        table.addCell(
            "search.scroll_time",
            "alias:scti,searchScrollTime;default:false;text-align:right;desc:time scroll contexts held open"
        );
        table.addCell("search.scroll_total", "alias:scto,searchScrollTotal;default:false;text-align:right;desc:completed scroll contexts");

        table.addCell(
            "search.point_in_time_current",
            "alias:scc,searchPointInTimeCurrent;default:false;text-align:right;desc:open point in time contexts"
        );
        table.addCell(
            "search.point_in_time_time",
            "alias:scti,searchPointInTimeTime;default:false;text-align:right;desc:time point in time contexts held open"
        );
        table.addCell(
            "search.point_in_time_total",
            "alias:scto,searchPointInTimeTotal;default:false;text-align:right;desc:completed point in time contexts"
        );

        table.addCell("segments.count", "alias:sc,segmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("segments.memory", "alias:sm,segmentsMemory;default:false;text-align:right;desc:memory used by segments");
        table.addCell(
            "segments.index_writer_memory",
            "alias:siwm,segmentsIndexWriterMemory;default:false;text-align:right;desc:memory used by index writer"
        );
        table.addCell(
            "segments.version_map_memory",
            "alias:svmm,segmentsVersionMapMemory;default:false;text-align:right;desc:memory used by version map"
        );
        table.addCell(
            "segments.fixed_bitset_memory",
            "alias:sfbm,fixedBitsetMemory;default:false;text-align:right;desc:memory used by fixed bit sets for nested object field types"
                + " and type filters for types referred in _parent fields"
        );

        table.addCell("suggest.current", "alias:suc,suggestCurrent;default:false;text-align:right;desc:number of current suggest ops");
        table.addCell("suggest.time", "alias:suti,suggestTime;default:false;text-align:right;desc:time spend in suggest");
        table.addCell("suggest.total", "alias:suto,suggestTotal;default:false;text-align:right;desc:number of suggest ops");

        table.endHeaders();
        return table;
    }

    Table buildTable(
        boolean fullId,
        RestRequest req,
        ClusterStateResponse state,
        NodesInfoResponse nodesInfo,
        NodesStatsResponse nodesStats
    ) {

        DiscoveryNodes nodes = state.getState().nodes();
        String clusterManagerId = nodes.getClusterManagerNodeId();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : nodes) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
            NodeStats stats = nodesStats.getNodesMap().get(node.getId());

            JvmInfo jvmInfo = info == null ? null : info.getInfo(JvmInfo.class);
            JvmStats jvmStats = stats == null ? null : stats.getJvm();
            FsInfo fsInfo = stats == null ? null : stats.getFs();
            OsStats osStats = stats == null ? null : stats.getOs();
            ProcessStats processStats = stats == null ? null : stats.getProcess();
            NodeIndicesStats indicesStats = stats == null ? null : stats.getIndices();

            table.startRow();

            table.addCell(fullId ? node.getId() : Strings.substring(node.getId(), 0, 4));
            table.addCell(info == null ? null : info.getInfo(ProcessInfo.class).getId());
            table.addCell(node.getHostAddress());
            table.addCell(node.getAddress().address().getPort());
            final HttpInfo httpInfo = info == null ? null : info.getInfo(HttpInfo.class);
            if (httpInfo != null) {
                TransportAddress transportAddress = httpInfo.getAddress().publishAddress();
                table.addCell(NetworkAddress.format(transportAddress.address()));
            } else {
                table.addCell("-");
            }

            table.addCell(node.getVersion().toString());
            table.addCell(info == null ? null : info.getBuild().type().displayName());
            table.addCell(info == null ? null : info.getBuild().hash());
            table.addCell(jvmInfo == null ? null : jvmInfo.version());

            ByteSizeValue diskTotal = null;
            ByteSizeValue diskUsed = null;
            ByteSizeValue diskAvailable = null;
            String diskUsedPercent = null;
            if (fsInfo != null) {
                diskTotal = fsInfo.getTotal().getTotal();
                diskAvailable = fsInfo.getTotal().getAvailable();
                diskUsed = new ByteSizeValue(diskTotal.getBytes() - diskAvailable.getBytes());

                double diskUsedRatio = diskTotal.getBytes() == 0 ? 1.0 : (double) diskUsed.getBytes() / diskTotal.getBytes();
                diskUsedPercent = String.format(Locale.ROOT, "%.2f", 100.0 * diskUsedRatio);
            }
            table.addCell(diskTotal);
            table.addCell(diskUsed);
            table.addCell(diskAvailable);
            table.addCell(diskUsedPercent);

            table.addCell(jvmStats == null ? null : jvmStats.getMem().getHeapUsed());
            table.addCell(jvmStats == null ? null : jvmStats.getMem().getHeapUsedPercent());
            table.addCell(jvmInfo == null ? null : jvmInfo.getMem().getHeapMax());
            table.addCell(osStats == null ? null : osStats.getMem() == null ? null : osStats.getMem().getUsed());
            table.addCell(osStats == null ? null : osStats.getMem() == null ? null : osStats.getMem().getUsedPercent());
            table.addCell(osStats == null ? null : osStats.getMem() == null ? null : osStats.getMem().getTotal());
            table.addCell(processStats == null ? null : processStats.getOpenFileDescriptors());
            table.addCell(
                processStats == null
                    ? null
                    : calculatePercentage(processStats.getOpenFileDescriptors(), processStats.getMaxFileDescriptors())
            );
            table.addCell(processStats == null ? null : processStats.getMaxFileDescriptors());

            table.addCell(osStats == null ? null : Short.toString(osStats.getCpu().getPercent()));
            boolean hasLoadAverage = osStats != null && osStats.getCpu().getLoadAverage() != null;
            table.addCell(
                !hasLoadAverage || osStats.getCpu().getLoadAverage()[0] == -1
                    ? null
                    : String.format(Locale.ROOT, "%.2f", osStats.getCpu().getLoadAverage()[0])
            );
            table.addCell(
                !hasLoadAverage || osStats.getCpu().getLoadAverage()[1] == -1
                    ? null
                    : String.format(Locale.ROOT, "%.2f", osStats.getCpu().getLoadAverage()[1])
            );
            table.addCell(
                !hasLoadAverage || osStats.getCpu().getLoadAverage()[2] == -1
                    ? null
                    : String.format(Locale.ROOT, "%.2f", osStats.getCpu().getLoadAverage()[2])
            );
            table.addCell(jvmStats == null ? null : jvmStats.getUptime());

            final String roles;
            final String allRoles;
            if (node.getRoles().isEmpty()) {
                roles = "-";
                allRoles = "-";
            } else {
                List<DiscoveryNodeRole> knownNodeRoles = node.getRoles()
                    .stream()
                    .filter(DiscoveryNodeRole::isKnownRole)
                    .collect(Collectors.toList());
                roles = knownNodeRoles.size() > 0
                    ? knownNodeRoles.stream().map(DiscoveryNodeRole::roleNameAbbreviation).sorted().collect(Collectors.joining())
                    : "-";
                allRoles = node.getRoles().stream().map(DiscoveryNodeRole::roleName).sorted().collect(Collectors.joining(","));
            }
            table.addCell(roles);
            table.addCell(allRoles);
            table.addCell(clusterManagerId == null ? "x" : clusterManagerId.equals(node.getId()) ? "*" : "-");
            table.addCell(node.getName());

            CompletionStats completionStats = indicesStats == null ? null : stats.getIndices().getCompletion();
            table.addCell(completionStats == null ? null : completionStats.getSize());

            FieldDataStats fdStats = indicesStats == null ? null : stats.getIndices().getFieldData();
            table.addCell(fdStats == null ? null : fdStats.getMemorySize());
            table.addCell(fdStats == null ? null : fdStats.getEvictions());

            QueryCacheStats fcStats = indicesStats == null ? null : indicesStats.getQueryCache();
            table.addCell(fcStats == null ? null : fcStats.getMemorySize());
            table.addCell(fcStats == null ? null : fcStats.getEvictions());
            table.addCell(fcStats == null ? null : fcStats.getHitCount());
            table.addCell(fcStats == null ? null : fcStats.getMissCount());

            RequestCacheStats qcStats = indicesStats == null ? null : indicesStats.getRequestCache();
            table.addCell(qcStats == null ? null : qcStats.getMemorySize());
            table.addCell(qcStats == null ? null : qcStats.getEvictions());
            table.addCell(qcStats == null ? null : qcStats.getHitCount());
            table.addCell(qcStats == null ? null : qcStats.getMissCount());

            FlushStats flushStats = indicesStats == null ? null : indicesStats.getFlush();
            table.addCell(flushStats == null ? null : flushStats.getTotal());
            table.addCell(flushStats == null ? null : flushStats.getTotalTime());

            GetStats getStats = indicesStats == null ? null : indicesStats.getGet();
            table.addCell(getStats == null ? null : getStats.current());
            table.addCell(getStats == null ? null : getStats.getTime());
            table.addCell(getStats == null ? null : getStats.getCount());
            table.addCell(getStats == null ? null : getStats.getExistsTime());
            table.addCell(getStats == null ? null : getStats.getExistsCount());
            table.addCell(getStats == null ? null : getStats.getMissingTime());
            table.addCell(getStats == null ? null : getStats.getMissingCount());

            IndexingStats indexingStats = indicesStats == null ? null : indicesStats.getIndexing();
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getDeleteCurrent());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getDeleteTime());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getDeleteCount());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getIndexCurrent());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getIndexTime());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getIndexCount());
            table.addCell(indexingStats == null ? null : indexingStats.getTotal().getIndexFailedCount());

            MergeStats mergeStats = indicesStats == null ? null : indicesStats.getMerge();
            table.addCell(mergeStats == null ? null : mergeStats.getCurrent());
            table.addCell(mergeStats == null ? null : mergeStats.getCurrentNumDocs());
            table.addCell(mergeStats == null ? null : mergeStats.getCurrentSize());
            table.addCell(mergeStats == null ? null : mergeStats.getTotal());
            table.addCell(mergeStats == null ? null : mergeStats.getTotalNumDocs());
            table.addCell(mergeStats == null ? null : mergeStats.getTotalSize());
            table.addCell(mergeStats == null ? null : mergeStats.getTotalTime());

            RefreshStats refreshStats = indicesStats == null ? null : indicesStats.getRefresh();
            table.addCell(refreshStats == null ? null : refreshStats.getTotal());
            table.addCell(refreshStats == null ? null : refreshStats.getTotalTime());
            table.addCell(refreshStats == null ? null : refreshStats.getExternalTotal());
            table.addCell(refreshStats == null ? null : refreshStats.getExternalTotalTime());
            table.addCell(refreshStats == null ? null : refreshStats.getListeners());

            ScriptStats scriptStats = stats == null ? null : stats.getScriptStats();
            table.addCell(scriptStats == null ? null : scriptStats.getCompilations());
            table.addCell(scriptStats == null ? null : scriptStats.getCacheEvictions());
            table.addCell(scriptStats == null ? null : scriptStats.getCompilationLimitTriggered());

            SearchStats searchStats = indicesStats == null ? null : indicesStats.getSearch();
            table.addCell(searchStats == null ? null : searchStats.getTotal().getFetchCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getFetchTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getFetchCount());
            table.addCell(searchStats == null ? null : searchStats.getOpenContexts());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getQueryCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getQueryTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getQueryCount());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getConcurrentQueryCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getConcurrentQueryTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getConcurrentQueryCount());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getConcurrentAvgSliceCount());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getScrollCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getScrollTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getScrollCount());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getPitCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getPitTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getPitCount());

            SegmentsStats segmentsStats = indicesStats == null ? null : indicesStats.getSegments();
            table.addCell(segmentsStats == null ? null : segmentsStats.getCount());
            table.addCell(segmentsStats == null ? null : segmentsStats.getZeroMemory());
            table.addCell(segmentsStats == null ? null : segmentsStats.getIndexWriterMemory());
            table.addCell(segmentsStats == null ? null : segmentsStats.getVersionMapMemory());
            table.addCell(segmentsStats == null ? null : segmentsStats.getBitsetMemory());

            table.addCell(searchStats == null ? null : searchStats.getTotal().getSuggestCurrent());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getSuggestTime());
            table.addCell(searchStats == null ? null : searchStats.getTotal().getSuggestCount());

            table.endRow();
        }

        return table;
    }

    /**
     * Calculate the percentage of {@code used} from the {@code max} number.
     * @param used The currently used number.
     * @param max The maximum number.
     * @return 0 if {@code max} is &lt;= 0. Otherwise 100 * {@code used} / {@code max}.
     */
    private short calculatePercentage(long used, long max) {
        return max <= 0 ? 0 : (short) ((100d * used) / max);
    }
}
