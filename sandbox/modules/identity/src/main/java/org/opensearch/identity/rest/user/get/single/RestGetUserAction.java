/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.get.single;

import org.opensearch.client.node.NodeClient;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.util.List;

import static java.util.Arrays.asList;
import static org.opensearch.identity.utils.RoutesHelper.addRoutesPrefix;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action for getting a single user
 */
public class RestGetUserAction extends BaseRestHandler {

    @Override
    public String getName() {
        return IdentityRestConstants.IDENTITY_GET_USER_ACTION;
    }

    /**
     * Rest request handler for creating a new user
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to be executed See {@link #handleRequest(RestRequest, RestChannel, NodeClient) for more}
     *
     * ````
     * Sample Request:
     * curl -XGET http://new-user:password@localhost:9200/_identity/api/users/apitest
     *
     *
     * Sample Response
     *
     * {
     *   "user": {
     *       "username": apitest,
     *       "attributes": {},
     *       "permissions": []
     *     }
     *   ]
     * }
     * ````
     */
    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String username = request.param("name");
        GetUserRequest getUserRequest = new GetUserRequest(username);

        return channel -> client.doExecute(GetUserAction.INSTANCE, getUserRequest, new RestStatusToXContentListener<>(channel));
    }

    /**
     * Routes to be registered for this action
     * @return the unmodifiable list of routes to be registered
     */
    @Override
    public List<Route> routes() {
        // e.g. return value "_identity/api/users/test"
        return addRoutesPrefix(asList(new Route(GET, "/users/{name}")));
    }

}
