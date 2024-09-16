/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.ProtobufTaskId;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.ProtobufTaskListener;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for a transport action
*
* @opensearch.internal
*/
public abstract class ProtobufTransportAction<Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> {

    public final String actionName;
    private final ProtobufActionFilter[] filters;
    protected final TaskManager taskManager;
    /**
     * @deprecated declare your own logger.
    */
    @Deprecated
    protected Logger logger = LogManager.getLogger(getClass());

    protected ProtobufTransportAction(String actionName, ProtobufActionFilters actionFilters, TaskManager taskManager) {
        this.actionName = actionName;
        this.filters = actionFilters.filters();
        this.taskManager = taskManager;
    }

    private Releasable registerChildNode(ProtobufTaskId parentTask) {
        if (parentTask.isSet()) {
            return taskManager.registerProtobufChildNode(parentTask.getId(), taskManager.localNode());
        } else {
            return () -> {};
        }
    }

    /**
     * Use this method when the transport action call should result in creation of a new task associated with the call.
    *
    * This is a typical behavior.
    */
    public final ProtobufTask execute(Request request, ActionListener<Response> listener) {
        /*
        * While this version of execute could delegate to the ProtobufTaskListener
        * version of execute that'd add yet another layer of wrapping on the
        * listener and prevent us from using the listener bare if there isn't a
        * task. That just seems like too many objects. Thus the two versions of
        * this method.
        */
        final Releasable unregisterChildNode = registerChildNode(request.getProtobufParentTask());
        final ProtobufTask task;

        try {
            task = taskManager.registerProtobuf("transport", actionName, request);
            System.out.println("Protobuf task registered from execute is " + task);
        } catch (TaskCancelledException e) {
            unregisterChildNode.close();
            throw e;
        }

        ThreadContext.StoredContext storedContext = taskManager.protobufTaskExecutionStarted(task);
        try {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregisterProtobufTask(task));
                    } finally {
                        listener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregisterProtobufTask(task));
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
     * Execute the transport action on the local node, returning the {@link ProtobufTask} used to track its execution and accepting a
    * {@link ProtobufTaskListener} which listens for the completion of the action.
    */
    public final ProtobufTask execute(Request request, ProtobufTaskListener<Response> listener) {
        final Releasable unregisterChildNode = registerChildNode(request.getProtobufParentTask());
        final ProtobufTask task;
        try {
            task = taskManager.registerProtobuf("transport", actionName, request);
        } catch (TaskCancelledException e) {
            unregisterChildNode.close();
            throw e;
        }
        ThreadContext.StoredContext storedContext = taskManager.protobufTaskExecutionStarted(task);
        try {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregisterProtobufTask(task));
                    } finally {
                        listener.onResponse(task, response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        Releasables.close(unregisterChildNode, () -> taskManager.unregisterProtobufTask(task));
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
    public final void execute(ProtobufTask task, Request request, ActionListener<Response> listener) {
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

    protected abstract void doExecute(ProtobufTask task, Request request, ActionListener<Response> listener);

    /**
     * A request filter chain
    *
    * @opensearch.internal
    */
    private static class RequestFilterChain<Request extends ProtobufActionRequest, Response extends ProtobufActionResponse>
        implements
            ProtobufActionFilterChain<Request, Response> {

        private final ProtobufTransportAction<Request, Response> action;
        private final AtomicInteger index = new AtomicInteger();
        private final Logger logger;

        private RequestFilterChain(ProtobufTransportAction<Request, Response> action, Logger logger) {
            this.action = action;
            this.logger = logger;
        }

        @Override
        public void proceed(ProtobufTask task, String actionName, Request request, ActionListener<Response> listener) {
            int i = index.getAndIncrement();
            try {
                if (i < this.action.filters.length) {
                    this.action.filters[i].apply(task, actionName, request, listener, this);
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
    private static class TaskResultStoringActionListener<Response extends ProtobufActionResponse> implements ActionListener<Response> {
        private final ActionListener<Response> delegate;
        private final ProtobufTask task;
        private final TaskManager taskManager;

        private TaskResultStoringActionListener(TaskManager taskManager, ProtobufTask task, ActionListener<Response> delegate) {
            this.taskManager = taskManager;
            this.task = task;
            this.delegate = delegate;
        }

        @Override
        public void onResponse(Response response) {
            try {
                taskManager.storeResultProtobuf(task, response, delegate);
            } catch (Exception e) {
                delegate.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                taskManager.storeResultProtobuf(task, e, delegate);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                delegate.onFailure(inner);
            }
        }
    }
}
