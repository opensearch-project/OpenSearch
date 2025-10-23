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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.tasks.TaskManager;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for a transport action
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> {

    public final String actionName;
    private final ActionFilter[] filters;
    protected final TaskManager taskManager;
    /**
     * @deprecated declare your own logger.
     */
    @Deprecated
    protected Logger logger = LogManager.getLogger(getClass());

    protected TransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
        this.actionName = actionName;
        this.filters = actionFilters.filters();
        this.taskManager = taskManager;
    }

    private Releasable registerChildNode(TaskId parentTask) {
        if (parentTask.isSet()) {
            return taskManager.registerChildNode(parentTask.getId(), taskManager.localNode());
        } else {
            return () -> {};
        }
    }

    /**
     * Use this method when the transport action call should result in creation of a new task associated with the call.
     * <p>
     * This is a typical behavior.
     */
    public final Task execute(Request request, ActionListener<Response> listener) {
        /*
         * While this version of execute could delegate to the TaskListener
         * version of execute that'd add yet another layer of wrapping on the
         * listener and prevent us from using the listener bare if there isn't a
         * task. That just seems like too many objects. Thus the two versions of
         * this method.
         */
        final Releasable unregisterChildNode = registerChildNode(request.getParentTask());
        final Task task;

        try {
            task = taskManager.register("transport", actionName, request);
        } catch (TaskCancelledException e) {
            unregisterChildNode.close();
            throw e;
        }

        ThreadContext.StoredContext storedContext = taskManager.taskExecutionStarted(task);
        try {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                    } finally {
                        listener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                    } finally {
                        listener.onFailure(e);
                    }
                }
            });
        } finally {
            storedContext.close();
        }

        return task;
    }

    /**
     * Execute the transport action on the local node, returning the {@link Task} used to track its execution and accepting a
     * {@link TaskListener} which listens for the completion of the action.
     */
    public final Task execute(Request request, TaskListener<Response> listener) {
        final Releasable unregisterChildNode = registerChildNode(request.getParentTask());
        final Task task;
        try {
            task = taskManager.register("transport", actionName, request);
        } catch (TaskCancelledException e) {
            unregisterChildNode.close();
            throw e;
        }
        ThreadContext.StoredContext storedContext = taskManager.taskExecutionStarted(task);
        try {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                    } finally {
                        listener.onResponse(task, response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                    } finally {
                        listener.onFailure(task, e);
                    }
                }
            });
        } finally {
            storedContext.close();
        }
        return task;
    }

    /**
     * Use this method when the transport action should continue to run in the context of the current task
     */
    public final void execute(Task task, Request request, ActionListener<Response> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }

        if (task != null && request.getShouldStoreResult()) {
            listener = new TaskResultStoringActionListener<>(taskManager, task, listener);
        }

        RequestFilterChain<Request, Response> requestFilterChain = new RequestFilterChain<>(this, logger);
        requestFilterChain.proceed(task, actionName, request, listener);
    }

    protected abstract void doExecute(Task task, Request request, ActionListener<Response> listener);

    /**
     * A request filter chain
     *
     * @opensearch.internal
     */
    private static class RequestFilterChain<Request extends ActionRequest, Response extends ActionResponse>
        implements
            ActionFilterChain<Request, Response> {

        private final TransportAction<Request, Response> action;
        private final AtomicInteger index = new AtomicInteger();
        private final Logger logger;

        private RequestFilterChain(TransportAction<Request, Response> action, Logger logger) {
            this.action = action;
            this.logger = logger;
        }

        @Override
        public void proceed(Task task, String actionName, Request request, ActionListener<Response> listener) {
            int i = index.getAndIncrement();
            try {
                if (i < this.action.filters.length) {
                    this.action.filters[i].apply(task, actionName, request, new ActionRequestMetadata<>(action, request), listener, this);
                } else if (i == this.action.filters.length) {
                    this.action.doExecute(task, request, listener);
                } else {
                    listener.onFailure(new IllegalStateException("proceed was called too many times"));
                }
            } catch (Exception e) {
                logger.trace("Error during transport action execution.", e);
                listener.onFailure(e);
            }
        }

    }

    /**
     * Wrapper for an action listener that stores the result at the end of the execution
     *
     * @opensearch.internal
     */
    private static class TaskResultStoringActionListener<Response extends ActionResponse> implements ActionListener<Response> {
        private final ActionListener<Response> delegate;
        private final Task task;
        private final TaskManager taskManager;

        private TaskResultStoringActionListener(TaskManager taskManager, Task task, ActionListener<Response> delegate) {
            this.taskManager = taskManager;
            this.task = task;
            this.delegate = delegate;
        }

        @Override
        public void onResponse(Response response) {
            try {
                taskManager.storeResult(task, response, delegate);
            } catch (Exception e) {
                delegate.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                taskManager.storeResult(task, e, delegate);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                delegate.onFailure(inner);
            }
        }
    }
}
