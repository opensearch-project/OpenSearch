/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.node;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.PluginContextSwitcher;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.threadpool.ThreadPool;

/**
 * Client that executes actions on the local node with contextual information about the plugin executing
 * transport actions.
 *
 * @opensearch.api
 */
public class PluginNodeClient extends NodeClient {
    private final PluginContextSwitcher contextSwitcher;

    public PluginNodeClient(Settings settings, ThreadPool threadPool, Plugin plugin) {
        super(settings, threadPool);
        contextSwitcher = new PluginContextSwitcher(threadPool, plugin);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        try (ThreadContext.StoredContext storedContext = contextSwitcher.switchContext()) {
            // Discard the task because the Client interface doesn't use it.
            executeLocally(action, request, listener);
        }
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link ActionListener}.
     * Prefer this method if you don't need access to the task when listening for the response. This is the method used to implement
     * the {@link Client} interface.
     *
     * This client will execute the transport action in the context of the plugin executing this action
     */
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        Task task;
        try (ThreadContext.StoredContext storedContext = contextSwitcher.switchContext()) {
            // Discard the task because the Client interface doesn't use it.
            task = transportAction(action).execute(request, listener);
        }
        return task;
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link TaskListener}. Prefer this
     * method if you need access to the task when listening for the response.
     *
     * This client will execute the transport action in the context of the plugin executing this action
     */
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
        ActionType<Response> action,
        Request request,
        TaskListener<Response> listener
    ) {
        Task task;
        try (ThreadContext.StoredContext storedContext = contextSwitcher.switchContext()) {
            task = transportAction(action).execute(request, listener);
        }
        return task;
    }
}
