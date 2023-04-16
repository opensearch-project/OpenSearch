/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequest;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestRemoteStoreStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_remote_store/stats"),
                new Route(GET, "/_remote_store/stats/{index}"),
                new Route(GET, "/_remote_store/stats/{index}/{shardId}")
            )
        );
    }
    @Override
    public String getName() {
        return "remote_store_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String shardId = request.param("shardId");
        String onlyLocal = request.param("local");
        RemoteStoreStatsRequest remoteStoreStatsRequest = new RemoteStoreStatsRequest();
        remoteStoreStatsRequest.all();
        if (index != null) {
            remoteStoreStatsRequest.indices(index);
        }
        if (shardId != null) {
            remoteStoreStatsRequest.shards(shardId);
        }
        return channel -> client.admin().cluster().remoteStoreStats(remoteStoreStatsRequest, new RestToXContentListener<>(channel));
    }

//    static final Map<String, Consumer<CommonStatsFlags>> FLAGS;
//
//    static {
//        final Map<String, Consumer<CommonStatsFlags>> flags = new HashMap<>();
//        for (final CommonStatsFlags.Flag flag : CommonStatsFlags.Flag.values()) {
//            flags.put(flag.getRestName(), f -> f.set(flag, true));
//        }
//        FLAGS = Collections.unmodifiableMap(flags);
//    }

//    protected void documentation(StringBuilder sb) {
//        sb.append("/_remote_store/stats\n");
//        sb.append("/_remote_store/stats/{index}\n");
//        sb.append("/_remote_store/stats/{index}/{shardId}\n");
//    }
//
//    protected Table getTableWithHeader(final RestRequest request) {
//        Table table = new Table();
//        table.startHeaders()
//            .addCell("shardId", "default:true;alias:i,idx;desc:index name")
//            .addCell("local_refresh_seq_no", "default:true;alias:i,idx;desc:index name")
//            .addCell("local_refresh_time", "default:true;alias:s,sh;desc:shard name")
//            .addCell("remote_refresh_seq_no", "default:true;alias:s,sh;desc:shard name")
//            .addCell("remote_refresh_time", "default:true;alias:s,sh;desc:shard name")
//            .addCell("upload_bytes_started", "default:true;alias:s,sh;desc:shard name")
//            .addCell("upload_bytes_failed", "default:true;alias:s,sh;desc:shard name")
//            .addCell("upload_bytes_succeeded", "default:true;alias:s,sh;desc:shard name")
//            .addCell("total_uploads_started", "default:true;alias:s,sh;desc:shard name")
//            .addCell("total_uploads_failed", "default:true;alias:s,sh;desc:shard name")
//            .addCell("total_uploads_succeeded", "default:true;alias:s,sh;desc:shard name")
//            .addCell("uploadTimeMovingAverage", "default:true;alias:s,sh;desc:shard name")
//            .addCell("uploadBytesPerSecondMovingAverage", "default:true;alias:s,sh;desc:shard name")
//            .addCell("uploadBytesMovingAverage", "default:true;alias:s,sh;desc:shard name");
////            .addCell("segmentFileBytesMap", "default:true;alias:s,sh;desc:shard name")
////            .addCell("segmentFileName", "default:true;alias:s,sh;desc:shard name");
//
//        table.endHeaders();
//        return table;
//    }
//
//    Table buildTable(RestRequest request, RemoteStoreStatsResponse stats) {
//        Table table = getTableWithHeader(request);
//        Arrays.stream(stats.getShards()).forEach(shardStats -> {
//            if (shardStats.getStats() == null) {
//                return;
//            }
//            table.startRow();
//            table.addCell(shardStats.getStats().getShardId());
//            table.addCell(shardStats.getStats().getLocalRefreshSeqNo());
//            table.addCell(shardStats.getStats().getLocalRefreshTime());
//            table.addCell(shardStats.getStats().getRemoteRefreshSeqNo());
//            table.addCell(shardStats.getStats().getRemoteRefreshTime());
//            table.addCell(shardStats.getStats().getUploadBytesStarted());
//            table.addCell(shardStats.getStats().getUploadBytesFailed());
//            table.addCell(shardStats.getStats().getUploadBytesSucceeded());
//            table.addCell(shardStats.getStats().getTotalUploadsStarted());
//            table.addCell(shardStats.getStats().getTotalUploadsFailed());
//            table.addCell(shardStats.getStats().getTotalUploadsSucceeded());
//            table.addCell(shardStats.getStats().getUploadTimeAverage());
//            table.addCell(shardStats.getStats().getUploadBytesPerSecondMovingAverage());
//            table.addCell(shardStats.getStats().getUploadBytesAverage());
//            table.endRow();
//        });
//        return table;
//    }
}
