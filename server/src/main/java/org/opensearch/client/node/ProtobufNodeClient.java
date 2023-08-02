/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.client.node;

import org.opensearch.action.ActionType;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ProtobufActionType;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionModule.ProtobufDynamicActionRegistry;
import org.opensearch.action.support.ProtobufTransportAction;
import org.opensearch.client.ProtobufClient;
import org.opensearch.client.support.ProtobufAbstractClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.ProtobufTaskListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterService;

import java.util.function.Supplier;

/**
 * ProtobufClient that executes actions on the local node.
*
* @opensearch.internal
*/
public class ProtobufNodeClient extends ProtobufAbstractClient {

    private ProtobufDynamicActionRegistry actionRegistry;
    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
    * {@link #executeLocally(ProtobufActionType, ProtobufActionRequest, ProtobufTaskListener)}.
    */
    private Supplier<String> localNodeId;
    private RemoteClusterService remoteClusterService;
    private NamedWriteableRegistry namedWriteableRegistry;

    public ProtobufNodeClient(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
    }

    public void initialize(
        ProtobufDynamicActionRegistry actionRegistry,
        Supplier<String> localNodeId,
        RemoteClusterService remoteClusterService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.actionRegistry = actionRegistry;
        this.localNodeId = localNodeId;
        this.remoteClusterService = remoteClusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void doExecute(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        // Discard the task because the ProtobufClient interface doesn't use it.
        executeLocally(action, request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link ProtobufTask} used to track it, and linking an {@link ActionListener}.
    * Prefer this method if you don't need access to the task when listening for the response. This is the method used to implement
    * the {@link ProtobufClient} interface.
    */
    public <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> ProtobufTask executeLocally(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        return transportAction(action).execute(request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link ProtobufTask} used to track it, and linking an {@link ProtobufTaskListener}. Prefer this
    * method if you need access to the task when listening for the response.
    */
    public <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> ProtobufTask executeLocally(
        ProtobufActionType<Response> action,
        Request request,
        ProtobufTaskListener<Response> listener
    ) {
        return transportAction(action).execute(request, listener);
    }

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
    * {@link #executeLocally(ProtobufActionType, ProtobufActionRequest, ProtobufTaskListener)}.
    */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    /**
     * Get the {@link ProtobufTransportAction} for an {@link ActionType}, throwing exceptions if the action isn't available.
    */
    @SuppressWarnings("unchecked")
    private <
        Request extends ProtobufActionRequest,
        Response extends ProtobufActionResponse> ProtobufTransportAction<Request, Response> transportAction(
            ProtobufActionType<Response> action
        ) {
        if (actionRegistry == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        ProtobufTransportAction<Request, Response> transportAction = (ProtobufTransportAction<Request, Response>) actionRegistry.get(
            action
        );
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction;
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
