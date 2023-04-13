/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.RemoteStoreStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Strings;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.action.cat.RestTable;

import java.io.IOException;
import java.sql.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestRemoteStoreStatsAction extends AbstractCatAction {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestRemoteStoreStatsAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_cat/remote_store/{index}"),
                new Route(GET, "/_cat/remote_store/{index}/{shardId}")
            )
        );
    }
    @Override
    public String getName() {
        return "remote_store_stats";
    }

    static final Map<String, Consumer<CommonStatsFlags>> FLAGS;

    static {
        final Map<String, Consumer<CommonStatsFlags>> flags = new HashMap<>();
        for (final CommonStatsFlags.Flag flag : CommonStatsFlags.Flag.values()) {
            flags.put(flag.getRestName(), f -> f.set(flag, true));
        }
        FLAGS = Collections.unmodifiableMap(flags);
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        String index = request.param("index");
        String shardId = request.param("shardId");
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterStateRequest, request, deprecationLogger, getName());
        clusterStateRequest.clear().nodes(true).routingTable(true).indices(index);
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                List<ShardRouting> allShards = clusterStateResponse.getState().getRoutingTable().allShards(index);
                Set<String> nodeIds = new HashSet<>();
                allShards.forEach(shardRouting -> {
                    nodeIds.add(shardRouting.currentNodeId());
                });
                RemoteStoreStatsRequest remoteStoreStatsRequest = new RemoteStoreStatsRequest();
                remoteStoreStatsRequest.all();
                remoteStoreStatsRequest.indices(index);
                client.admin().cluster().remoteStoreStats(remoteStoreStatsRequest, new RestResponseListener<RemoteStoreStatsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(RemoteStoreStatsResponse remoteStoreStatsResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(request, clusterStateResponse, remoteStoreStatsResponse), channel);
                    }
                });

            }

        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/remote_store/{index}\n");
        sb.append("/_cat/remote_store/{index}/{shardIds}\n");
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders()
            .addCell("shardId", "default:true;alias:i,idx;desc:index name")
            .addCell("local_refresh_seq_no", "default:true;alias:i,idx;desc:index name")
            .addCell("local_refresh_time", "default:true;alias:s,sh;desc:shard name")
            .addCell("remote_refresh_seq_no", "default:true;alias:s,sh;desc:shard name")
            .addCell("remote_refresh_time", "default:true;alias:s,sh;desc:shard name")
            .addCell("upload_bytes_started", "default:true;alias:s,sh;desc:shard name")
            .addCell("upload_bytes_failed", "default:true;alias:s,sh;desc:shard name")
            .addCell("upload_bytes_succeeded", "default:true;alias:s,sh;desc:shard name")
            .addCell("total_uploads_started", "default:true;alias:s,sh;desc:shard name")
            .addCell("total_uploads_succeeded", "default:true;alias:s,sh;desc:shard name")
            .addCell("uploadTimeMovingAverage", "default:true;alias:s,sh;desc:shard name")
            .addCell("uploadBytesPerSecondMovingAverage", "default:true;alias:s,sh;desc:shard name")
            .addCell("uploadBytesMovingAverage", "default:true;alias:s,sh;desc:shard name");
//            .addCell("segmentFileBytesMap", "default:true;alias:s,sh;desc:shard name")
//            .addCell("segmentFileName", "default:true;alias:s,sh;desc:shard name");

        table.endHeaders();
        return table;
    }

    Table buildTable(RestRequest request, ClusterStateResponse state, RemoteStoreStatsResponse stats) {
        Table table = getTableWithHeader(request);
        Arrays.stream(stats.getShards()).forEach(shardStats -> {
            table.startRow();
            table.addCell(shardStats.getStats().getShardId());
            table.addCell(shardStats.getStats().getLocalRefreshSeqNo());
            table.addCell(shardStats.getStats().getLocalRefreshTime());
            table.addCell(shardStats.getStats().getRemoteRefreshSeqNo());
            table.addCell(shardStats.getStats().getRemoteRefreshTime());
            table.addCell(shardStats.getStats().getUploadBytesStarted());
            table.addCell(shardStats.getStats().getUploadBytesSucceeded());
            table.addCell(shardStats.getStats().getTotalUploadsStarted());
            table.addCell(shardStats.getStats().getTotalUploadsSucceeded());
            table.addCell(shardStats.getStats().getTotalUploadsFailed());
            table.addCell(shardStats.getStats().getUploadTimeAverage());
            table.addCell(shardStats.getStats().getUploadBytesPerSecondMovingAverage());
            table.addCell(shardStats.getStats().getUploadBytesAverage());
            table.endRow();
        });
        return table;
    }
//    @Override
//    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
//
//        nodesStatsRequest.timeout(request.param("timeout"));
//
//        if (metrics.size() == 1 && metrics.contains("_all")) {
//            if (request.hasParam("index_metric")) {
//                throw new IllegalArgumentException(
//                    String.format(
//                        Locale.ROOT,
//                        "request [%s] contains index metrics [%s] but all stats requested",
//                        request.path(),
//                        request.param("index_metric")
//                    )
//                );
//            }
//            nodesStatsRequest.all();
//            nodesStatsRequest.indices(CommonStatsFlags.ALL);
//        } else if (metrics.contains("_all")) {
//            throw new IllegalArgumentException(
//                String.format(
//                    Locale.ROOT,
//                    "request [%s] contains _all and individual metrics [%s]",
//                    request.path(),
//                    request.param("metric")
//                )
//            );
//        } else {
//            nodesStatsRequest.clear();
//
//            // use a sorted set so the unrecognized parameters appear in a reliable sorted order
//            final Set<String> invalidMetrics = new TreeSet<>();
//            for (final String metric : metrics) {
//                final Consumer<RemoteStoreStatsRequest> handler = METRICS.get(metric);
//                if (handler != null) {
//                    handler.accept(nodesStatsRequest);
//                } else {
//                    invalidMetrics.add(metric);
//                }
//            }
//
//            if (!invalidMetrics.isEmpty()) {
//                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keySet(), "metric"));
//            }
//
//            // check for index specific metrics
//            if (metrics.contains("indices")) {
//                Set<String> indexMetrics = Strings.tokenizeByCommaToSet(request.param("index_metric", "_all"));
//                if (indexMetrics.size() == 1 && indexMetrics.contains("_all")) {
//                    nodesStatsRequest.indices(CommonStatsFlags.ALL);
//                } else {
//                    CommonStatsFlags flags = new CommonStatsFlags();
//                    flags.clear();
//                    // use a sorted set so the unrecognized parameters appear in a reliable sorted order
//                    final Set<String> invalidIndexMetrics = new TreeSet<>();
//                    for (final String indexMetric : indexMetrics) {
//                        final Consumer<CommonStatsFlags> handler = FLAGS.get(indexMetric);
//                        if (handler != null) {
//                            handler.accept(flags);
//                        } else {
//                            invalidIndexMetrics.add(indexMetric);
//                        }
//                    }
//
//                    if (!invalidIndexMetrics.isEmpty()) {
//                        throw new IllegalArgumentException(unrecognized(request, invalidIndexMetrics, FLAGS.keySet(), "index metric"));
//                    }
//
//                    nodesStatsRequest.indices(flags);
//                }
//            } else if (request.hasParam("index_metric")) {
//                throw new IllegalArgumentException(
//                    String.format(
//                        Locale.ROOT,
//                        "request [%s] contains index metrics [%s] but indices stats not requested",
//                        request.path(),
//                        request.param("index_metric")
//                    )
//                );
//            }
//        }
//
//        if (nodesStatsRequest.indices().isSet(CommonStatsFlags.Flag.FieldData) && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
//            nodesStatsRequest.indices()
//                .fieldDataFields(request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", null)));
//        }
//        if (nodesStatsRequest.indices().isSet(CommonStatsFlags.Flag.Completion) && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
//            nodesStatsRequest.indices()
//                .completionDataFields(request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", null)));
//        }
//        if (nodesStatsRequest.indices().isSet(CommonStatsFlags.Flag.Search) && (request.hasParam("groups"))) {
//            nodesStatsRequest.indices().groups(request.paramAsStringArray("groups", null));
//        }
//        if (nodesStatsRequest.indices().isSet(CommonStatsFlags.Flag.Segments)) {
//            nodesStatsRequest.indices().includeSegmentFileSizes(request.paramAsBoolean("include_segment_file_sizes", false));
//        }
//        if (request.hasParam("include_all")) {
//            nodesStatsRequest.indices().includeAllShardIndexingPressureTrackers(request.paramAsBoolean("include_all", false));
//        }
//
//        if (request.hasParam("top")) {
//            nodesStatsRequest.indices().includeOnlyTopIndexingPressureMetrics(request.paramAsBoolean("top", false));
//        }
//
//        return channel -> client.admin().cluster().remoteStoreStats(nodesStatsRequest, new RestActions.NodesResponseRestListener<>(channel));
//    }
}
