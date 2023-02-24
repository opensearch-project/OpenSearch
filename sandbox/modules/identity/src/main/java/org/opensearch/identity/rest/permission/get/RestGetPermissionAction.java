/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.get;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.client.node.NodeClient;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.opensearch.identity.utils.RoutesHelper.addRoutesPrefix;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action for checking the permissions of a subject in the permission store
 */
public class RestGetPermissionAction extends BaseRestHandler {

    /**
     * @return a string of the action name  -- "_identity/api/permissions/get
     */
    @Override
    public String getName() {
        // permission_action_get
        return IdentityRestConstants.PERMISSION_ACTION_PREFIX + "_get";
    }

    /**
     * Rest request handler for checking the permissions of a subject
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to be executed See {#handleRequest(RestRequest, RestChannel, NodeClient) for more}
     * @throws IOException if errors encountered when parsing from XContent
     *
     * ````
     * Sample Request:
     * curl -XGET http://new-user:password@localhost:9200/_identity/api/permissions/new-user/get  -H"Content-type: application/json"
     *
     *
     * Sample Response
     *
     * {
     *   "permissions": [
     *     {
     *       "successful": true,
     *       "username": "new-user",
     *       "permission list": ["my_permission1", "my_permission2"]
     *
     *     }
     *   ]
     * }
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String username = request.param("username");

        // Parsing request body using DefaultObjectMapper
        JsonNode contentAsNode;
        GetPermissionRequest getPermissionRequest = new GetPermissionRequest(username);

        // TODO: check if this bypass to directly doExecute is okay.
        // TODO: Ideally, this should be registered as `createUser` request in Client.java and AbstractClient.java
        // TODO: see if you can add to RequestConverters.java to follow convention
        return channel -> client.doExecute(GetPermissionAction.INSTANCE, getPermissionRequest, new RestStatusToXContentListener<>(channel));
    }

    /**
     * Routes to be registered for this action
     * @return the unmodifiable list of routes to be registered
     */
    @Override
    public List<Route> routes() {
        // e.g. return value "/permissions/{username}" which is then added to "_identity/api"

        return addRoutesPrefix(asList(new Route(GET, IdentityRestConstants.PERMISSION_SUBPATH + "/{username}")));
    }
}
