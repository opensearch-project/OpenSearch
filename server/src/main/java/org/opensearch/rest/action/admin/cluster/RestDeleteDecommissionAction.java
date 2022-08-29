/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionRequest;
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
import static org.opensearch.rest.RestRequest.Method.DELETE;

public class RestDeleteDecommissionAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestDeleteDecommissionAction.class);

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(300L);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(DELETE, "/_cluster/decommission/awareness/{awareness_attribute_name}/{awareness_attribute_value}"));
    }

    @Override
    public String getName() {
        return "delete_decommission_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeleteDecommissionRequest deleteDecommissionRequest = createRequest(request);
        return channel -> client.admin().cluster().deleteDecommission(deleteDecommissionRequest, new RestToXContentListener<>(channel));
    }

    DeleteDecommissionRequest createRequest(RestRequest request) {
        String attributeName = null;
        String attributeValue = null;
        DeleteDecommissionRequest deleteDecommissionRequest = Requests.deleteDecommissionRequest();
        if (request.hasParam("awareness_attribute_name")) {
            attributeName = request.param("awareness_attribute_name");
        }

        if (request.hasParam("awareness_attribute_value")) {
            attributeValue = request.param("awareness_attribute_value");
        }
        deleteDecommissionRequest.setDecommissionAttribute(new DecommissionAttribute(attributeName, attributeValue));
        deleteDecommissionRequest.setTimeout(
            TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT, getClass().getSimpleName() + ".timeout")
        );
        return deleteDecommissionRequest;
    }
}
