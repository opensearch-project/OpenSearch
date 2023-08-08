/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsRequest;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Table;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentOpenSearchExtension;
import org.opensearch.core.common.Strings;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * RestCatSegmentReplicationAction provides information about the status of replica's segment replication event
 * in a string format, designed to be used at the command line. An Index can
 * be specified to limit output to a particular index or indices.
 *
 * @opensearch.api
 */
public class RestCatSegmentReplicationAction extends AbstractCatAction {
    @Override
    public List<RestHandler.Route> routes() {
        return unmodifiableList(
            asList(new RestHandler.Route(GET, "/_cat/segment_replication"), new RestHandler.Route(GET, "/_cat/segment_replication/{index}"))
        );
    }

    @Override
    public String getName() {
        return "cat_segment_replication_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/segment_replication\n");
        sb.append("/_cat/segment_replication/{index}\n");
    }

    @Override
    public BaseRestHandler.RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        segmentReplicationStatsRequest.timeout(request.param("timeout"));
        segmentReplicationStatsRequest.detailed(request.paramAsBoolean("detailed", false));
        segmentReplicationStatsRequest.shards(Strings.splitStringByCommaToArray(request.param("shards")));
        segmentReplicationStatsRequest.activeOnly(request.paramAsBoolean("active_only", false));
        segmentReplicationStatsRequest.indicesOptions(IndicesOptions.fromRequest(request, segmentReplicationStatsRequest.indicesOptions()));

        return channel -> client.admin()
            .indices()
            .segmentReplicationStats(segmentReplicationStatsRequest, new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(final SegmentReplicationStatsResponse response) throws Exception {
                    return RestTable.buildResponse(buildSegmentReplicationTable(request, response), channel);
                }
            });
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {

        boolean detailed = false;
        if (request != null) {
            detailed = Boolean.parseBoolean(request.param("detailed"));
        }

        Table t = new Table();
        t.startHeaders()
            .addCell("shardId", "alias:s;desc: shard Id")
            .addCell("target_node", "alias:tnode;desc:target node name")
            .addCell("target_host", "alias:thost;desc:target host")
            .addCell("checkpoints_behind", "alias:cpb;desc:checkpoints behind primary")
            .addCell("bytes_behind", "alias:bb;desc:bytes behind primary")
            .addCell("current_lag", "alias:clag;desc:ongoing time elapsed waiting for replica to catch up to primary")
            .addCell("last_completed_lag", "alias:lcl;desc:time taken for replica to catch up to latest primary refresh")
            .addCell("rejected_requests", "alias:rr;desc:count of rejected requests for the replication group");
        if (detailed) {
            t.addCell("stage", "alias:st;desc:segment replication event stage")
                .addCell("time", "alias:t,ti;desc:current replication event time")
                .addCell("files_fetched", "alias:ff;desc:files fetched")
                .addCell("files_percent", "alias:fp;desc:percent of files fetched")
                .addCell("bytes_fetched", "alias:bf;desc:bytes fetched")
                .addCell("bytes_percent", "alias:bp;desc:percent of bytes fetched")
                .addCell("start_time", "alias:start;desc:segment replication start time")
                .addCell("stop_time", "alias:stop;desc:segment replication stop time")
                .addCell("files", "alias:f;desc:number of files to fetch")
                .addCell("files_total", "alias:tf;desc:total number of files")
                .addCell("bytes", "alias:b;desc:number of bytes to fetch")
                .addCell("bytes_total", "alias:tb;desc:total number of bytes")
                .addCell("replicating_stage_time_taken", "alias:rstt;desc:time taken in replicating stage")
                .addCell("get_checkpoint_info_stage_time_taken", "alias:gcistt;desc:time taken in get checkpoint info stage")
                .addCell("file_diff_stage_time_taken", "alias:fdstt;desc:time taken in file diff stage")
                .addCell("get_files_stage_time_taken", "alias:gfstt;desc:time taken in get files stage")
                .addCell("finalize_replication_stage_time_taken", "alias:frstt;desc:time taken in finalize replication stage");
        }
        t.endHeaders();
        return t;
    }

    /**
     * buildSegmentReplicationTable will build a table of SegmentReplication information suitable
     * for displaying at the command line.
     *
     * @param request  A Rest request
     * @param response A SegmentReplication status response
     * @return A table containing index, shardId, node, target size, fetched size and percentage for each fetching replica
     */
    public Table buildSegmentReplicationTable(RestRequest request, SegmentReplicationStatsResponse response) {
        boolean detailed = false;
        if (request != null) {
            detailed = Boolean.parseBoolean(request.param("detailed"));
        }
        Table t = getTableWithHeader(request);

        for (Map.Entry<String, List<SegmentReplicationPerGroupStats>> entry : response.getReplicationStats().entrySet()) {
            final List<SegmentReplicationPerGroupStats> replicationPerGroupStats = entry.getValue();

            if (replicationPerGroupStats.isEmpty()) {
                continue;
            }

            // Sort ascending by shard id for readability
            CollectionUtil.introSort(replicationPerGroupStats, (o1, o2) -> {
                int id1 = o1.getShardId().id();
                int id2 = o2.getShardId().id();
                return Integer.compare(id1, id2);
            });

            for (SegmentReplicationPerGroupStats perGroupStats : replicationPerGroupStats) {

                final Set<SegmentReplicationShardStats> replicaShardStats = perGroupStats.getReplicaStats();

                for (SegmentReplicationShardStats shardStats : replicaShardStats) {
                    final SegmentReplicationState state = shardStats.getCurrentReplicationState();
                    if (state == null) {
                        continue;
                    }

                    t.startRow();
                    t.addCell(perGroupStats.getShardId());
                    // these nulls should never happen, here for safety.
                    t.addCell(state.getTargetNode().getName());
                    t.addCell(state.getTargetNode().getHostName());
                    t.addCell(shardStats.getCheckpointsBehindCount());
                    t.addCell(new ByteSizeValue(shardStats.getBytesBehindCount()));
                    t.addCell(new TimeValue(shardStats.getCurrentReplicationTimeMillis()));
                    t.addCell(new TimeValue(shardStats.getLastCompletedReplicationTimeMillis()));
                    t.addCell(perGroupStats.getRejectedRequestCount());
                    if (detailed) {
                        t.addCell(state.getStage().toString().toLowerCase(Locale.ROOT));
                        t.addCell(new TimeValue(state.getTimer().time()));
                        t.addCell(state.getIndex().recoveredFileCount());
                        t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredFilesPercent()));
                        t.addCell(state.getIndex().recoveredBytes());
                        t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredBytesPercent()));
                        t.addCell(XContentOpenSearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().startTime()));
                        t.addCell(XContentOpenSearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().stopTime()));
                        t.addCell(state.getIndex().totalRecoverFiles());
                        t.addCell(state.getIndex().totalFileCount());
                        t.addCell(new ByteSizeValue(state.getIndex().totalRecoverBytes()));
                        t.addCell(new ByteSizeValue(state.getIndex().totalBytes()));
                        t.addCell(state.getReplicatingStageTime());
                        t.addCell(state.getGetCheckpointInfoStageTime());
                        t.addCell(state.getFileDiffStageTime());
                        t.addCell(state.getGetFileStageTime());
                        t.addCell(state.getFinalizeReplicationStageTime());
                    }
                    t.endRow();
                }
            }
        }

        return t;
    }
}
