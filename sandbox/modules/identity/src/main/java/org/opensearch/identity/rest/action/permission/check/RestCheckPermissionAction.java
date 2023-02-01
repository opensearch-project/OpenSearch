/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.check;

import org.opensearch.client.node.NodeClient;
import org.opensearch.identity.rest.RestConfigConstants;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.opensearch.identity.utils.RoutesHelper.addRoutesPrefix;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestCheckPermissionAction extends BaseRestHandler {

    @Override
    public String getName() {
        return RestConfigConstants.IDENTITY_READ_PERMISSION_ACTION;
    }

    /**
     * Rest request handler for checking the permissions associated with a principal
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to be executed See {@link #handleRequest(RestRequest, RestChannel, NodeClient) for more}
     * @throws IOException
     */
    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String principalString = request.param("principalString");


        // Parsing request body using DefaultObjectMapper
        // JsonNode contentAsNode = DefaultObjectMapper.readTree(request.content().utf8ToString());
        // String permissionString = contentAsNode.get("permissionString").asText();
        //
        // CheckPermissionRequest checkPermissionRequest = new CheckPermissionRequest(permissionString);

        // // Parsing request body using XContentParser

        CheckPermissionRequest checkPermissionRequest = new CheckPermissionRequest();
        checkPermissionRequest.setPrincipalString(principalString);
        request.withContentOrSourceParamParserOrNull((xContentParser -> {
            if (xContentParser != null) {
                try {
                    checkPermissionRequest.fromXContent(xContentParser);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to parse check permission request body", e);
                }
            }
        }));

        // TODO: check if this bypass to directly doExecute is okay.
        // TODO: Ideally, this should be registered as `checkPermission` request in Client.java and AbstractClient.java
        // TODO: see if you can add to RequestConverters.java to follow convention
        return channel -> client.doExecute(
            CheckPermissionAction.INSTANCE,
            checkPermissionRequest,
            new RestStatusToXContentListener<>(channel)
        );
    }

    /**
     * Routes to be registered for this action
     * @return the unmodifiable list of routes to be registered
     */
    @Override
    public List<Route> routes() {
        // e.g. return value "_identity/api/internalusers/test"
        return addRoutesPrefix(asList(new Route(GET, "/permissions")));
    }
}
