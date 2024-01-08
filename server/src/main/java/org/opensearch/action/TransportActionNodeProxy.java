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

package org.opensearch.action;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

/**
 * A generic proxy that will execute the given action against a specific node.
 *
 * @opensearch.internal
 */
public class TransportActionNodeProxy<Request extends ActionRequest, Response extends ActionResponse> {

    private final TransportService transportService;
    private final ActionType<Response> action;
    private final TransportRequestOptions transportOptions;

    public TransportActionNodeProxy(Settings settings, ActionType<Response> action, TransportService transportService) {
        this.action = action;
        this.transportService = transportService;
        this.transportOptions = action.transportOptions(settings);
    }

    public void execute(final DiscoveryNode node, final Request request, final ActionListener<Response> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        transportService.sendRequest(
            node,
            action.name(),
            request,
            transportOptions,
            new ActionListenerResponseHandler<>(listener, action.getResponseReader())
        );
    }
}
