/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * No-op transport action for index-level authorization checks. If execution reaches
 * {@code doExecute}, the security filter has already authorized the request — we simply
 * return success. If the security filter denies access, it short-circuits before this
 * method is called and the caller receives a security exception.
 */
public class TransportAnalyticsAuthAction extends HandledTransportAction<AnalyticsAuthRequest, AnalyticsAuthResponse> {

    @Inject
    public TransportAnalyticsAuthAction(TransportService transportService, ActionFilters actionFilters) {
        super(AnalyticsAuthAction.NAME, transportService, actionFilters, AnalyticsAuthRequest::new);
    }

    @Override
    protected void doExecute(Task task, AnalyticsAuthRequest request, ActionListener<AnalyticsAuthResponse> listener) {
        listener.onResponse(new AnalyticsAuthResponse());
    }
}
