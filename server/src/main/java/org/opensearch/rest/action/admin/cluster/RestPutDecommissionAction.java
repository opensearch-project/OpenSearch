/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.decommission.awareness.put.PutDecommissionRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Registers decommission action
 *
 * @opensearch.api
 */
public class RestPutDecommissionAction extends BaseRestHandler {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(300L);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/_cluster/decommission/awareness/{awareness_attribute_name}/{awareness_attribute_value}"));
    }

    @Override
    public String getName() {
        return "put_decommission_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PutDecommissionRequest putDecommissionRequest = createRequest(request);
        return channel -> client.admin().cluster().putDecommission(putDecommissionRequest, new RestToXContentListener<>(channel));
    }

    PutDecommissionRequest createRequest(RestRequest request) throws IOException {
        String attributeName = null;
        String attributeValue = null;
        PutDecommissionRequest putDecommissionRequest = Requests.putDecommissionRequest();
        if (request.hasParam("awareness_attribute_name")) {
            attributeName = request.param("awareness_attribute_name");
        }

        if (request.hasParam("awareness_attribute_value")) {
            attributeValue = request.param("awareness_attribute_value");
        }
        return putDecommissionRequest.setDecommissionAttribute(new DecommissionAttribute(attributeName, attributeValue))
            .setTimeout(TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT, getClass().getSimpleName() + ".timeout"));
    }
}
