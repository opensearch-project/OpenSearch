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
import org.opensearch.action.ActionModule.DynamicActionRegistry;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.TransportAction;
import org.opensearch.client.Client;
import org.opensearch.client.support.AbstractClient;
import org.opensearch.client.support.ProtobufAbstractClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.threadpool.ProtobufThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterService;

import java.util.function.Supplier;

/**
 * Client that executes actions on the local node.
*
* @opensearch.internal
*/
public class ProtobufNodeClient extends ProtobufAbstractClient {

    private DynamicActionRegistry actionRegistry;
    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
    * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
    */
    private Supplier<String> localNodeId;
    private RemoteClusterService remoteClusterService;
    private NamedWriteableRegistry namedWriteableRegistry;

    public ProtobufNodeClient(Settings settings, ProtobufThreadPool threadPool) {
        super(settings, threadPool);
    }

    public void initialize(
        DynamicActionRegistry actionRegistry,
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
        // Discard the task because the Client interface doesn't use it.
        executeLocally(action, request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link ActionListener}.
    * Prefer this method if you don't need access to the task when listening for the response. This is the method used to implement
    * the {@link Client} interface.
    */
    public <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> Task executeLocally(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        return transportAction(action).execute(request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link TaskListener}. Prefer this
    * method if you need access to the task when listening for the response.
    */
    public <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> Task executeLocally(
        ProtobufActionType<Response> action,
        Request request,
        TaskListener<Response> listener
    ) {
        return transportAction(action).execute(request, listener);
    }

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
    * {@link #executeLocally(ProtobufActionType, ProtobufActionRequest, TaskListener)}.
    */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    /**
     * Get the {@link TransportAction} for an {@link ActionType}, throwing exceptions if the action isn't available.
    */
    @SuppressWarnings("unchecked")
    private <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> TransportAction<Request, Response> transportAction(
        ProtobufActionType<Response> action
    ) {
        if (actionRegistry == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        TransportAction<Request, Response> transportAction = (TransportAction<Request, Response>) actionRegistry.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
