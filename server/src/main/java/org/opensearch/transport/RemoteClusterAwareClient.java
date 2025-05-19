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

package org.opensearch.transport;

import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.support.AbstractClient;

/**
 * Client that is aware of remote clusters
 *
 * @opensearch.internal
 */
final class RemoteClusterAwareClient extends AbstractClient {

    private final TransportService service;
    private final String clusterAlias;
    private final RemoteClusterService remoteClusterService;

    RemoteClusterAwareClient(Settings settings, ThreadPool threadPool, TransportService service, String clusterAlias) {
        super(settings, threadPool);
        this.service = service;
        this.clusterAlias = clusterAlias;
        this.remoteClusterService = service.getRemoteClusterService();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        remoteClusterService.ensureConnected(clusterAlias, ActionListener.wrap(v -> {
            Transport.Connection connection;
            if (request instanceof RemoteClusterAwareRequest) {
                DiscoveryNode preferredTargetNode = ((RemoteClusterAwareRequest) request).getPreferredTargetNode();
                connection = remoteClusterService.getConnection(preferredTargetNode, clusterAlias);
            } else {
                connection = remoteClusterService.getConnection(clusterAlias);
            }
            service.sendRequest(
                connection,
                action.name(),
                request,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, action.getResponseReader())
            );
        }, listener::onFailure));
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }
}
