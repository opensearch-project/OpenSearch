/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateResponse;
import org.opensearch.client.node.ProtobufNodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.server.proto.ClusterStateResponseProto;
import org.opensearch.server.proto.NodesInfoProto.NodesInfo;
import org.opensearch.server.proto.NodesStatsProto.NodesStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.FlushStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.GetStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.IndexingStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.MergeStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.QueryCacheStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.RefreshStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.RequestCacheStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.ScriptStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.SearchStats;
import org.opensearch.server.proto.NodesStatsProto.NodesStats.SegmentStats;

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
public class ProtobufRestNodesAction extends ProtobufAbstractCatAction {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ProtobufRestNodesAction.class);
    static final String LOCAL_DEPRECATED_MESSAGE = "Deprecated parameter [local] used. This parameter does not cause this API to act "
        + "locally, and should not be used. It will be unsupported in version 8.0.";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/nodes_protobuf"));
    }

    @Override
    public String getName() {
        return "cat_nodes_protobuf_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/nodes_protobuf\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final ProtobufNodeClient client) {
        final ProtobufClusterStateRequest clusterStateRequest = new ProtobufClusterStateRequest();
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
        return channel -> client.admin()
            .cluster()
            .state(clusterStateRequest, new RestActionListener<ProtobufClusterStateResponse>(channel) {
                @Override
                public void processResponse(final ProtobufClusterStateResponse clusterStateResponse) {
                    ProtobufNodesInfoRequest nodesInfoRequest = new ProtobufNodesInfoRequest();
                    nodesInfoRequest.addMetrics(
                        request.param("timeout"),
                        ProtobufNodesInfoRequest.Metric.JVM.metricName(),
                        ProtobufNodesInfoRequest.Metric.OS.metricName(),
                        ProtobufNodesInfoRequest.Metric.PROCESS.metricName(),
                        ProtobufNodesInfoRequest.Metric.HTTP.metricName()
                    );
                    client.admin().cluster().nodesInfo(nodesInfoRequest, new RestActionListener<ProtobufNodesInfoResponse>(channel) {
                        @Override
                        public void processResponse(final ProtobufNodesInfoResponse nodesInfoResponse) {
                            ProtobufNodesStatsRequest nodesStatsRequest = new ProtobufNodesStatsRequest();
                            nodesStatsRequest.timeout(request.param("timeout"));
                            nodesStatsRequest.clear()
                                .indices(true)
                                .addMetrics(
                                    ProtobufNodesStatsRequest.Metric.JVM.metricName(),
                                    ProtobufNodesStatsRequest.Metric.OS.metricName(),
                                    ProtobufNodesStatsRequest.Metric.FS.metricName(),
                                    ProtobufNodesStatsRequest.Metric.PROCESS.metricName(),
                                    ProtobufNodesStatsRequest.Metric.SCRIPT.metricName()
                                );
                            client.admin()
                                .cluster()
                                .nodesStats(nodesStatsRequest, new RestResponseListener<ProtobufNodesStatsResponse>(channel) {
                                    @Override
                                    public RestResponse buildResponse(ProtobufNodesStatsResponse nodesStatsResponse) throws Exception {
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

        table.addCell("version", "default:false;alias:v;desc:es version");
        table.addCell("type", "default:false;alias:t;desc:es distribution type");
        table.addCell("build", "default:false;alias:b;desc:es build hash");
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
        ProtobufClusterStateResponse state,
        ProtobufNodesInfoResponse nodesInfo,
        ProtobufNodesStatsResponse nodesStats
    ) {
        ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes nodes = state.response().getClusterState().getNodes();
        String clusterManagerId = nodes.getClusterManagerNodeId();
        Table table = getTableWithHeader(req);
        for (ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node node : nodes.getAllNodesList()) {
            NodesInfo info = nodesInfo.nodesMap().get(node.getNodeId());
            NodesStats stats = nodesStats.nodesMap().get(node.getNodeId());

            table.startRow();

            table.addCell(fullId ? node.getNodeId() : Strings.substring(node.getNodeId(), 0, 4));
            table.addCell(info == null ? null : info.getProcessId());
            table.addCell(node.getHostAddress());
            table.addCell(node.getTransportAddress());
            table.addCell(info == null ? null : info.getAddress());

            table.addCell(node.getVersion().toString());
            table.addCell(info == null ? null : info.getDisplayName());
            table.addCell(info == null ? null : info.getHash());
            table.addCell(info == null ? null : info.getJvmInfoVersion());

            ByteSizeValue diskTotal = null;
            ByteSizeValue diskUsed = null;
            ByteSizeValue diskAvailable = null;
            String diskUsedPercent = null;
            if (stats != null) {
                diskTotal = new ByteSizeValue(stats.getDiskTotal());
                diskAvailable = new ByteSizeValue(stats.getDiskAvailable());
                diskUsed = new ByteSizeValue(diskTotal.getBytes() - diskAvailable.getBytes());
                double diskUsedRatio = diskTotal.getBytes() == 0 ? 1.0 : (double) diskUsed.getBytes() / diskTotal.getBytes();
                diskUsedPercent = String.format(Locale.ROOT, "%.2f", 100.0 * diskUsedRatio);
            }

            table.addCell(diskTotal);
            table.addCell(diskUsed);
            table.addCell(diskAvailable);
            table.addCell(diskUsedPercent);

            table.addCell(stats == null ? null : new ByteSizeValue(stats.getJvmHeapUsed()));
            table.addCell(stats == null ? null : stats.getJvmHeapUsedPercent());
            table.addCell(info == null ? null : info.getJvmHeapMax());
            table.addCell(stats == null ? null : new ByteSizeValue(stats.getOsMemUsed()));
            table.addCell(stats == null ? null : stats.getOsMemUsedPercent());
            table.addCell(stats == null ? null : new ByteSizeValue(stats.getOsMemTotal()));
            table.addCell(stats == null ? null : stats.getProcessOpenFileDescriptors());
            table.addCell(
                stats == null ? null : calculatePercentage(stats.getProcessOpenFileDescriptors(), stats.getProcessMaxFileDescriptors())
            );
            table.addCell(stats == null ? null : stats.getProcessMaxFileDescriptors());

            table.addCell(stats == null ? null : stats.getOsCpuPercent());
            boolean hasLoadAverage = stats != null && stats.getOsCpuLoadAverageList() != null;
            table.addCell(
                !hasLoadAverage || stats.getOsCpuLoadAverage(0) == -1
                    ? null
                    : String.format(Locale.ROOT, "%.2f", stats.getOsCpuLoadAverage(0))
            );
            table.addCell(
                !hasLoadAverage || stats.getOsCpuLoadAverage(1) == -1
                    ? null
                    : String.format(Locale.ROOT, "%.2f", stats.getOsCpuLoadAverage(1))
            );
            table.addCell(
                !hasLoadAverage || stats.getOsCpuLoadAverage(2) == -1
                    ? null
                    : String.format(Locale.ROOT, "%.2f", stats.getOsCpuLoadAverage(2))
            );
            table.addCell(stats == null ? null : new TimeValue(stats.getJvmUpTime()));

            final String roles;
            final String allRoles;
            if (node.getRolesList().isEmpty()) {
                roles = "-";
                allRoles = "-";
            } else {
                List<ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.NodeRole> knownNodeRoles = node
                    .getRolesList()
                    .stream()
                    .filter(ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.NodeRole::getIsKnownRole)
                    .collect(Collectors.toList());
                roles = knownNodeRoles.size() > 0
                    ? knownNodeRoles.stream()
                        .map(
                            ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.NodeRole::getRoleNameAbbreviation
                        )
                        .sorted()
                        .collect(Collectors.joining())
                    : "-";
                allRoles = node.getRolesList()
                    .stream()
                    .map(ClusterStateResponseProto.ClusterStateResponse.ClusterState.DiscoveryNodes.Node.NodeRole::getRoleName)
                    .sorted()
                    .collect(Collectors.joining(","));
            }
            table.addCell(roles);
            table.addCell(allRoles);
            table.addCell(clusterManagerId == null ? "x" : clusterManagerId.equals(node.getNodeId()) ? "*" : "-");
            table.addCell(node.getNodeName());

            table.addCell(stats == null ? null : stats.getCompletionStats() == null ? null : stats.getCompletionStats().getSize());

            table.addCell(stats == null ? null : stats.getFieldDataStats() == null ? null : stats.getFieldDataStats().getMemSize());
            table.addCell(stats == null ? null : stats.getFieldDataStats() == null ? null : stats.getFieldDataStats().getEvictions());

            QueryCacheStats fcStats = stats != null && stats.getQueryCacheStats() != null ? stats.getQueryCacheStats() : null;
            table.addCell(fcStats == null ? null : new ByteSizeValue(fcStats.getRamBytesUsed()));
            table.addCell(fcStats == null ? null : fcStats.getCacheCount() - fcStats.getCacheSize());
            table.addCell(fcStats == null ? null : fcStats.getHitCount());
            table.addCell(fcStats == null ? null : fcStats.getMissCount());

            RequestCacheStats qcStats = stats != null && stats.getRequestCacheStats() != null ? stats.getRequestCacheStats() : null;
            table.addCell(qcStats == null ? null : new ByteSizeValue(qcStats.getMemorySize()));
            table.addCell(qcStats == null ? null : qcStats.getEvictions());
            table.addCell(qcStats == null ? null : qcStats.getHitCount());
            table.addCell(qcStats == null ? null : qcStats.getMissCount());

            FlushStats flushStats = stats != null && stats.getFlushStats() != null ? stats.getFlushStats() : null;
            table.addCell(flushStats == null ? null : flushStats.getTotal());
            table.addCell(flushStats == null ? null : flushStats.getTotalTimeInMillis());

            GetStats getStats = stats != null && stats.getGetStats() != null ? stats.getGetStats() : null;
            table.addCell(getStats == null ? null : getStats.getCurrent());
            table.addCell(getStats == null ? null : new TimeValue(getStats.getTime()));
            table.addCell(getStats == null ? null : getStats.getCount());
            table.addCell(getStats == null ? null : new TimeValue(getStats.getExistsTimeInMillis()));
            table.addCell(getStats == null ? null : getStats.getExistsCount());
            table.addCell(getStats == null ? null : new TimeValue(getStats.getMissingTimeInMillis()));
            table.addCell(getStats == null ? null : getStats.getMissingCount());

            IndexingStats indexingStats = stats != null && stats.getIndexingStats() != null ? stats.getIndexingStats() : null;
            table.addCell(indexingStats == null ? null : indexingStats.getDeleteCurrent());
            table.addCell(indexingStats == null ? null : new TimeValue(indexingStats.getDeleteTimeInMillis()));
            table.addCell(indexingStats == null ? null : indexingStats.getDeleteCount());
            table.addCell(indexingStats == null ? null : indexingStats.getIndexCurrent());
            table.addCell(indexingStats == null ? null : new TimeValue(indexingStats.getIndexTimeInMillis()));
            table.addCell(indexingStats == null ? null : indexingStats.getIndexCount());
            table.addCell(indexingStats == null ? null : indexingStats.getIndexFailedCount());

            MergeStats mergeStats = stats != null && stats.getMergeStats() != null ? stats.getMergeStats() : null;
            table.addCell(mergeStats == null ? null : mergeStats.getCurrent());
            table.addCell(mergeStats == null ? null : mergeStats.getCurrentNumDocs());
            table.addCell(mergeStats == null ? null : new ByteSizeValue(mergeStats.getCurrentSizeInBytes()));
            table.addCell(mergeStats == null ? null : mergeStats.getTotal());
            table.addCell(mergeStats == null ? null : mergeStats.getTotalNumDocs());
            table.addCell(mergeStats == null ? null : new ByteSizeValue(mergeStats.getTotalSizeInBytes()));
            table.addCell(mergeStats == null ? null : new TimeValue(mergeStats.getTotalTimeInMillis()));

            RefreshStats refreshStats = stats != null && stats.getRefreshStats() != null ? stats.getRefreshStats() : null;
            table.addCell(refreshStats == null ? null : refreshStats.getTotal());
            table.addCell(refreshStats == null ? null : new TimeValue(refreshStats.getTotalTimeInMillis()));
            table.addCell(refreshStats == null ? null : refreshStats.getExternalTotal());
            table.addCell(refreshStats == null ? null : new TimeValue(refreshStats.getExternalTotalTimeInMillis()));
            table.addCell(refreshStats == null ? null : refreshStats.getListeners());

            ScriptStats scriptStats = stats != null && stats.getScriptStats() != null ? stats.getScriptStats() : null;
            table.addCell(scriptStats == null ? null : scriptStats.getCompilations());
            table.addCell(scriptStats == null ? null : scriptStats.getCacheEvictions());
            table.addCell(scriptStats == null ? null : scriptStats.getCompilationLimitTriggered());

            SearchStats searchStats = stats != null && stats.getSearchStats() != null ? stats.getSearchStats() : null;
            table.addCell(searchStats == null ? null : searchStats.getFetchCurrent());
            table.addCell(searchStats == null ? null : new TimeValue(searchStats.getFetchTimeInMillis()));
            table.addCell(searchStats == null ? null : searchStats.getFetchCount());
            table.addCell(searchStats == null ? null : searchStats.getOpenContexts());
            table.addCell(searchStats == null ? null : searchStats.getQueryCurrent());
            table.addCell(searchStats == null ? null : new TimeValue(searchStats.getQueryTimeInMillis()));
            table.addCell(searchStats == null ? null : searchStats.getQueryCount());
            table.addCell(searchStats == null ? null : searchStats.getScrollCurrent());
            table.addCell(searchStats == null ? null : new TimeValue(searchStats.getScrollTimeInMillis()));
            table.addCell(searchStats == null ? null : searchStats.getScrollCount());
            table.addCell(searchStats == null ? null : searchStats.getPitCurrent());
            table.addCell(searchStats == null ? null : new TimeValue(searchStats.getPitTimeInMillis()));
            table.addCell(searchStats == null ? null : searchStats.getPitCount());

            SegmentStats segmentsStats = stats != null && stats.getSegmentStats() != null ? stats.getSegmentStats() : null;
            table.addCell(segmentsStats == null ? null : segmentsStats.getCount());
            table.addCell(segmentsStats == null ? null : new ByteSizeValue(0L));
            table.addCell(segmentsStats == null ? null : new ByteSizeValue(segmentsStats.getIndexWriterMemoryInBytes()));
            table.addCell(segmentsStats == null ? null : new ByteSizeValue(segmentsStats.getVersionMapMemoryInBytes()));
            table.addCell(segmentsStats == null ? null : new ByteSizeValue(segmentsStats.getBitsetMemoryInBytes()));

            table.addCell(searchStats == null ? null : searchStats.getSuggestCurrent());
            table.addCell(searchStats == null ? null : new TimeValue(searchStats.getSuggestTimeInMillis()));
            table.addCell(searchStats == null ? null : searchStats.getSuggestCount());

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
