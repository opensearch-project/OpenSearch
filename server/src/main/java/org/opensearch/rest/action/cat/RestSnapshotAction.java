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

import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.transport.client.node.NodeClient;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Cat API class to display information about snapshots
 *
 * @opensearch.api
 */
public class RestSnapshotAction extends AbstractCatAction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestSnapshotAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/snapshots"), new Route(GET, "/_cat/snapshots/{repository}")));
    }

    @Override
    public String getName() {
        return "cat_snapshot_action";
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, NodeClient client) {
        GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest().repository(request.param("repository"))
            .snapshots(new String[] { GetSnapshotsRequest.ALL_SNAPSHOTS });

        getSnapshotsRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", getSnapshotsRequest.ignoreUnavailable()));

        getSnapshotsRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", getSnapshotsRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(getSnapshotsRequest, request, deprecationLogger, getName());

        return channel -> client.admin()
            .cluster()
            .getSnapshots(getSnapshotsRequest, new RestResponseListener<GetSnapshotsResponse>(channel) {
                @Override
                public RestResponse buildResponse(GetSnapshotsResponse getSnapshotsResponse) throws Exception {
                    return RestTable.buildResponse(buildTable(request, getSnapshotsResponse), channel);
                }
            });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/snapshots/{repository}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        return new Table().startHeaders()
            .addCell("id", "alias:id,snapshot;desc:unique snapshot")
            .addCell("status", "alias:s,status;text-align:right;desc:snapshot name")
            .addCell("start_epoch", "alias:ste,startEpoch;desc:start time in seconds since 1970-01-01 00:00:00")
            .addCell("start_time", "alias:sti,startTime;desc:start time in HH:MM:SS")
            .addCell("end_epoch", "alias:ete,endEpoch;desc:end time in seconds since 1970-01-01 00:00:00")
            .addCell("end_time", "alias:eti,endTime;desc:end time in HH:MM:SS")
            .addCell("duration", "alias:dur,duration;text-align:right;desc:duration")
            .addCell("indices", "alias:i,indices;text-align:right;desc:number of indices")
            .addCell("successful_shards", "alias:ss,successful_shards;text-align:right;desc:number of successful shards")
            .addCell("failed_shards", "alias:fs,failed_shards;text-align:right;desc:number of failed shards")
            .addCell("total_shards", "alias:ts,total_shards;text-align:right;desc:number of total shards")
            .addCell("reason", "default:false;alias:r,reason;desc:reason for failures")
            .endHeaders();
    }

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("HH:mm:ss").withZone(ZoneOffset.UTC);

    private Table buildTable(RestRequest req, GetSnapshotsResponse getSnapshotsResponse) {
        Table table = getTableWithHeader(req);
        for (SnapshotInfo snapshotStatus : getSnapshotsResponse.getSnapshots()) {
            table.startRow();

            table.addCell(snapshotStatus.snapshotId().getName());
            table.addCell(snapshotStatus.state());
            table.addCell(TimeUnit.SECONDS.convert(snapshotStatus.startTime(), TimeUnit.MILLISECONDS));
            table.addCell(FORMATTER.format(Instant.ofEpochMilli(snapshotStatus.startTime())));
            table.addCell(TimeUnit.SECONDS.convert(snapshotStatus.endTime(), TimeUnit.MILLISECONDS));
            table.addCell(FORMATTER.format(Instant.ofEpochMilli(snapshotStatus.endTime())));
            final long durationMillis;
            if (snapshotStatus.state() == SnapshotState.IN_PROGRESS) {
                durationMillis = System.currentTimeMillis() - snapshotStatus.startTime();
            } else {
                durationMillis = snapshotStatus.endTime() - snapshotStatus.startTime();
            }
            table.addCell(TimeValue.timeValueMillis(durationMillis));
            table.addCell(snapshotStatus.indices().size());
            table.addCell(snapshotStatus.successfulShards());
            table.addCell(snapshotStatus.failedShards());
            table.addCell(snapshotStatus.totalShards());
            table.addCell(snapshotStatus.reason());

            table.endRow();
        }

        return table;
    }
}
