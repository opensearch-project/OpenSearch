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
import org.opensearch.identity.Subject;
import org.opensearch.plugins.PluginSubject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.identity.AbstractSubject.SUBJECT_HEADER;

/**
 * Transport action for GetExecutionContext.
 *
 * Returns the canonical class name of the plugin that is currently executing the transport action.
 */
public class TestGetExecutionContextTransportAction extends HandledTransportAction<TestGetExecutionContextRequest, TestGetExecutionContextResponse> {
    private final TransportService transportService;
    private final PluginSubject pluginSubject;

    @Inject
    public TestGetExecutionContextTransportAction(TransportService transportService, ActionFilters actionFilters, PluginSubject pluginSubject) {
        super(TestGetExecutionContextAction.NAME, transportService, actionFilters, TestGetExecutionContextRequest::new);
        this.transportService = transportService;
        this.pluginSubject = pluginSubject;
    }

    @Override
    protected void doExecute(Task task, TestGetExecutionContextRequest request, ActionListener<TestGetExecutionContextResponse> listener) {
        try {
            pluginSubject.runAs(() -> {
                String pluginClassName = transportService.getThreadPool().getThreadContext().getHeader(SUBJECT_HEADER);
                listener.onResponse(new TestGetExecutionContextResponse(pluginClassName));
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
