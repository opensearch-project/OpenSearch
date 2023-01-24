/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.segment_replication.SegmentReplicationRequest;
import org.opensearch.action.admin.indices.segment_replication.SegmentReplicationResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.Strings;
import org.opensearch.common.Table;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentOpenSearchExtension;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestCatSegmentReplicationAction extends  AbstractCatAction {
    @Override
    public List<RestHandler.Route> routes() {
        return unmodifiableList(asList(new RestHandler.Route(GET, "/_cat/segment_replication"), new RestHandler.Route(GET, "/_cat/segment_replication/{index}")));
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
        String indexname = request.param("index");
        logger.info("index name is : {} ", indexname);
        final SegmentReplicationRequest segmentReplicationRequest = new SegmentReplicationRequest(Strings.splitStringByCommaToArray(request.param("index")));
        segmentReplicationRequest.timeout(request.param("timeout"));
        segmentReplicationRequest.detailed(request.paramAsBoolean("detailed", false));
        segmentReplicationRequest.activeOnly(request.paramAsBoolean("active_only", false));
        segmentReplicationRequest.indicesOptions(IndicesOptions.fromRequest(request, segmentReplicationRequest.indicesOptions()));

        return channel -> client.admin().indices().segment_replication(segmentReplicationRequest, new RestToXContentListener<>(channel) {
//            @Override
//            public RestResponse buildResponse(final SegmentReplicationResponse response) throws Exception {
//                return RestTable.buildResponse(buildSegmentReplicationTable(request, response), channel);
//            }
        });
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        return null;
    }

    /**
     * buildRecoveryTable will build a table of recovery information suitable
     * for displaying at the command line.
     *
     * @param request  A Rest request
     * @param response A recovery status response
     * @return A table containing index, shardId, node, target size, recovered size and percentage for each recovering replica
     */
    public Table buildRecoveryTable(RestRequest request, SegmentReplicationResponse response) {
        return null;
    }
}
