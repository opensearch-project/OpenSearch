/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.put;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.client.node.NodeClient;
import org.opensearch.identity.DefaultObjectMapper;
import org.opensearch.identity.rest.RestConstants;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.opensearch.identity.utils.RoutesHelper.addRoutesPrefix;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action for adding a permission to the permission store
 */
public class RestPutPermissionAction extends BaseRestHandler {

    /**
     * @return a string of the action name  -- "_identity/api/permissions/put
     */
    @Override
    public String getName() {
        // permission_action_put
        return RestConstants.PERMISSION_ACTION_PREFIX + "_put";
    }

    /**
     * Rest request handler for granting a permission to a subject
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to be executed See {#handleRequest(RestRequest, RestChannel, NodeClient) for more}
     * @throws IOException if errors encountered when parsing from XContent
     *
     * ````
     * Sample Request:
     * curl -XPUT http://new-user:password@localhost:9200/_identity/api/permissions/second_user/put --data '{ "permission" : "my_permission" }' -H"Content-type: application/json"
     *
     *
     * Sample Response
     *
     * {
     *   "permissions": [
     *     {
     *       "successful": true,
     *       "permission added": "my_permission",
     *       "username": "second_user"
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
        try {
            contentAsNode = DefaultObjectMapper.readTree(request.content().utf8ToString());
            String permission = contentAsNode.get("permission").asText();

            PutPermissionRequest putPermissionRequest = new PutPermissionRequest(username, permission);

            // TODO: check if this bypass to directly doExecute is okay.
            // TODO: Ideally, this should be registered as `createUser` request in Client.java and AbstractClient.java
            // TODO: see if you can add to RequestConverters.java to follow convention
            return channel -> client.doExecute(
                PutPermissionAction.INSTANCE,
                putPermissionRequest,
                new RestStatusToXContentListener<>(channel)
            );
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(ErrorType.BODY_NOT_PARSEABLE.getMessage() + "PUT Permission");
        }
    }

    /**
     * Routes to be registered for this action
     * @return the unmodifiable list of routes to be registered
     */
    @Override
    public List<Route> routes() {
        // e.g. return value "/permissions/{username}" which is then added to "_identity/api"

        return addRoutesPrefix(asList(new Route(PUT, RestConstants.PERMISSION_SUBPATH + "/{username}")));
    }
}
