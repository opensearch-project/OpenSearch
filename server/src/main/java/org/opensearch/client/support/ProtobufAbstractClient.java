/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.client.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ProtobufActionType;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoAction;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateAction;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.ProtobufClient;
import org.opensearch.client.ProtobufAdminClient;
import org.opensearch.client.ProtobufClusterAdminClient;
import org.opensearch.client.ProtobufOpenSearchClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

/**
 * Base client used to create concrete client implementations
*
* @opensearch.internal
*/
public abstract class ProtobufAbstractClient implements ProtobufClient {

    protected final Logger logger;

    protected final Settings settings;
    private final ThreadPool threadPool;
    private final Admin admin;

    public ProtobufAbstractClient(Settings settings, ThreadPool threadPool) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.admin = new Admin(this);
        this.logger = LogManager.getLogger(this.getClass());
    }

    @Override
    public final Settings settings() {
        return this.settings;
    }

    @Override
    public final ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override
    public final ProtobufAdminClient admin() {
        return admin;
    }

    @Override
    public final <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> ActionFuture<Response> execute(
        ProtobufActionType<Response> action,
        Request request
    ) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    /**
     * This is the single execution point of *all* clients.
    */
    @Override
    public final <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void execute(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        doExecute(action, request, listener);
    }

    protected abstract <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void doExecute(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );

    static class Admin implements ProtobufAdminClient {

        private final ClusterAdmin clusterAdmin;

        Admin(ProtobufOpenSearchClient client) {
            this.clusterAdmin = new ClusterAdmin(client);
        }

        @Override
        public ProtobufClusterAdminClient cluster() {
            return clusterAdmin;
        }
    }

    static class ClusterAdmin implements ProtobufClusterAdminClient {

        private final ProtobufOpenSearchClient client;

        ClusterAdmin(ProtobufOpenSearchClient client) {
            this.client = client;
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> ActionFuture<Response> execute(
            ProtobufActionType<Response> action,
            Request request
        ) {
            return client.execute(action, request);
        }

        @Override
        public <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void execute(
            ProtobufActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            client.execute(action, request, listener);
        }

        @Override
        public ActionFuture<ProtobufClusterStateResponse> state(final ProtobufClusterStateRequest request) {
            return execute(ProtobufClusterStateAction.INSTANCE, request);
        }

        @Override
        public void state(final ProtobufClusterStateRequest request, final ActionListener<ProtobufClusterStateResponse> listener) {
            execute(ProtobufClusterStateAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<ProtobufNodesInfoResponse> nodesInfo(final ProtobufNodesInfoRequest request) {
            return execute(ProtobufNodesInfoAction.INSTANCE, request);
        }

        @Override
        public void nodesInfo(final ProtobufNodesInfoRequest request, final ActionListener<ProtobufNodesInfoResponse> listener) {
            execute(ProtobufNodesInfoAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<ProtobufNodesStatsResponse> nodesStats(final ProtobufNodesStatsRequest request) {
            return execute(ProtobufNodesStatsAction.INSTANCE, request);
        }

        @Override
        public void nodesStats(final ProtobufNodesStatsRequest request, final ActionListener<ProtobufNodesStatsResponse> listener) {
            execute(ProtobufNodesStatsAction.INSTANCE, request, listener);
        }
    }
}
