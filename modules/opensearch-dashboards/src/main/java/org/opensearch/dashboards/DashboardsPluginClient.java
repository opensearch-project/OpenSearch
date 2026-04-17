/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.identity.Subject;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.FilterClient;

public class DashboardsPluginClient extends FilterClient {

    private Subject subject;

    public DashboardsPluginClient(Client delegate) {
        super(delegate);
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        if (subject == null) {
            throw new IllegalStateException("DashboardsPluginClient is not initialized.");
        }
        try {
            subject.runAs(() -> super.doExecute(action, request, listener));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
