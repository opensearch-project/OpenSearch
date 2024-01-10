/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.api.action;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportAPIAction extends HandledTransportAction<APIRequest, APIResponse> {

    @Inject
    public TransportAPIAction(Settings settings, TransportService transportService, ActionFilters actionFilters) {
        super(APIAction.NAME, transportService, actionFilters, APIRequest::new);
    }

    @Override
    protected void doExecute(Task task, APIRequest request, ActionListener<APIResponse> listener) {
        listener.onResponse(new APIResponse(Version.CURRENT, Build.CURRENT));
    }
}
