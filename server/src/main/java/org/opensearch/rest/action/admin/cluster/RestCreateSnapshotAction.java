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

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.transport.client.Requests.createSnapshotRequest;

/**
 * Creates a new snapshot
 *
 * @opensearch.api
 */
public class RestCreateSnapshotAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestCreateSnapshotAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(PUT, "/_snapshot/{repository}/{snapshot}"), new Route(POST, "/_snapshot/{repository}/{snapshot}"))
        );
    }

    @Override
    public String getName() {
        return "create_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CreateSnapshotRequest createSnapshotRequest = createSnapshotRequest(request.param("repository"), request.param("snapshot"));
        request.applyContentParser(p -> createSnapshotRequest.source(p.mapOrdered()));
        createSnapshotRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", createSnapshotRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(createSnapshotRequest, request, deprecationLogger, getName());
        createSnapshotRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", false));
        return channel -> client.admin().cluster().createSnapshot(createSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
