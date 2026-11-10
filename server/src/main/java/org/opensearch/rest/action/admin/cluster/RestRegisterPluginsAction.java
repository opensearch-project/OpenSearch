/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.registerplugins.RegisterPluginsAction;
import org.opensearch.action.admin.cluster.registerplugins.RegisterPluginsRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST action to register analysis components from loaded plugins
 *
 * @opensearch.api
 */
public class RestRegisterPluginsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_plugins/register")));
    }

    @Override
    public String getName() {
        return "register_plugins_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RegisterPluginsRequest registerPluginsRequest = new RegisterPluginsRequest();
        registerPluginsRequest.setRegisterAnalysis(request.paramAsBoolean("register_analysis", true));
        
        return channel -> client.admin().cluster().execute(RegisterPluginsAction.INSTANCE, registerPluginsRequest, new RestToXContentListener<>(channel));
    }
}
