/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.action.admin.cluster.snapshots.list.SnapshotIndicesListAction;
import org.opensearch.action.admin.cluster.snapshots.list.SnapshotIndicesListRequest;
import org.opensearch.action.admin.cluster.snapshots.list.SnapshotIndicesListResponse;
import org.opensearch.common.Table;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestSnapshotIndicesListAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_list/snapshot/{repository}/{snapshot}/indices")));
    }

    @Override
    public String getName() {
        return "list_snapshot_indices_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_list/snapshot/{repository}/{snapshot}/indices\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, NodeClient client) {
        final String repository = request.param("repository");
        final String snapshot = request.param("snapshot");
        final int from = Math.max(0, request.paramAsInt("from", 0));
        final int size = Math.max(1, request.paramAsInt("size", 500));

        SnapshotIndicesListRequest req = new SnapshotIndicesListRequest().repository(repository).snapshot(snapshot).from(from).size(size);
        return channel -> client.execute(SnapshotIndicesListAction.INSTANCE,
            req,
            new RestResponseListener<SnapshotIndicesListResponse>(channel) {
                @Override
                public org.opensearch.rest.RestResponse buildResponse(SnapshotIndicesListResponse response) throws Exception {
                    return org.opensearch.rest.action.cat.RestTable.buildResponse(buildTable(request, response), channel);
                }
            }
        );
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        return new Table().startHeaders()
            .addCell("index", "alias:index;desc:index name")
            .addCell("shards.total", "alias:st;desc:total shards")
            .addCell("shards.done", "alias:sd;desc:done shards")
            .addCell("shards.failed", "alias:sf;desc:failed shards")
            .addCell("file_count", "alias:fc;desc:file count")
            .addCell("size_in_bytes", "alias:s;desc:size in bytes")
            .addCell("start_time_in_millis", "alias:stm;desc:start time millis")
            .addCell("time_in_millis", "alias:tm;desc:time in millis")
            .endHeaders();
    }

    private Table buildTable(RestRequest request, SnapshotIndicesListResponse response) {
        Table t = getTableWithHeader(request);
        for (SnapshotIndicesListResponse.IndexRow r : response.rows()) {
            t.startRow();
            t.addCell(r.index);
            t.addCell(r.shardsTotal);
            t.addCell(r.shardsDone);
            t.addCell(r.shardsFailed);
            t.addCell(r.fileCount);
            t.addCell(r.sizeInBytes);
            t.addCell(r.startTimeInMillis);
            t.addCell(r.timeInMillis);
            t.endRow();
        }
        return t;
    }
}


