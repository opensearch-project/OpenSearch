/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class SampleTransportAction extends HandledTransportAction<SampleRequest, SampleResponse> {
    @Inject
    public SampleTransportAction(String actionName, ActionFilters actionFilters, TransportService transportService) {
        super(actionName, transportService, actionFilters, SampleRequest::new);
    }

    /**
     * @param task
     * @param request
     * @param listener
     */
    @Override
    protected void doExecute(Task task, SampleRequest request, ActionListener<SampleResponse> listener) {
        listener.onResponse(new SampleResponse());
    }


}
