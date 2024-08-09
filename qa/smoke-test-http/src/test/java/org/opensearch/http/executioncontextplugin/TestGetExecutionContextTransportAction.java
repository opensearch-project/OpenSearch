/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.executioncontextplugin;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for GetExecutionContext.
 *
 * Returns the canonical class name of the plugin that is currently executing the transport action.
 */
public class TestGetExecutionContextTransportAction extends HandledTransportAction<TestGetExecutionContextRequest, TestGetExecutionContextResponse> {
    private final TransportService transportService;

    @Inject
    public TestGetExecutionContextTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(TestGetExecutionContextAction.NAME, transportService, actionFilters, TestGetExecutionContextRequest::new);
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, TestGetExecutionContextRequest request, ActionListener<TestGetExecutionContextResponse> listener) {
        String pluginClassName = transportService.getThreadPool().getThreadContext().getHeader(ThreadContext.PLUGIN_EXECUTION_CONTEXT);
        listener.onResponse(new TestGetExecutionContextResponse(pluginClassName));
    }
}
