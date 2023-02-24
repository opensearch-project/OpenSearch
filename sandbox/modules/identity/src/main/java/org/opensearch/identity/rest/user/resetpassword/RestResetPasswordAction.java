/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.resetpassword;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.client.node.NodeClient;
import org.opensearch.identity.DefaultObjectMapper;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.opensearch.identity.utils.RoutesHelper.addRoutesPrefix;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Rest action for resetting password
 *
 * /users/{user}/resetpassword rest request action to create a user
 *
 * @opensearch.api
 */
public class RestResetPasswordAction extends BaseRestHandler {

    @Override
    public String getName() {
        return IdentityRestConstants.IDENTITY_RESET_USER_PASSWORD_ACTION;
    }

    /**
     * Rest request handler for creating a new user
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to be executed See {@link #handleRequest(RestRequest, RestChannel, NodeClient) for more}
     * @throws IOException if errors encountered when parsing from XContent
     *
     * ````
     * Sample Request:
     * curl -XPOST http://new-user:password@localhost:9200/_identity/api/users/test/resetpassword --data '{ "oldpassword" : "test", "newpassword" : "newtestpassword" }' -H"Content-type: application/json"
     *
     *
     * Sample Response
     *
     * {
     *   "successful":true,
     *   "username":"test",
     *   "message":"test user's password updated successfully."
     * }
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String username = request.param("name");

        // Parsing request body using DefaultObjectMapper
        JsonNode contentAsNode;
        try {
            contentAsNode = DefaultObjectMapper.readTree(request.content().utf8ToString());
            String oldPassword = contentAsNode.get("oldpassword").asText();
            String newPassword = contentAsNode.get("newpassword").asText();

            ResetPasswordRequest resetPasswordRequest = new ResetPasswordRequest(username, oldPassword, newPassword);
            return channel -> client.doExecute(
                ResetPasswordAction.INSTANCE,
                resetPasswordRequest,
                new RestStatusToXContentListener<>(channel)
            );
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(ErrorType.BODY_NOT_PARSEABLE.getMessage() + "POST " + getName());
        }
    }

    /**
     * Routes to be registered for this action
     * @return the unmodifiable list of routes to be registered
     */
    @Override
    public List<Route> routes() {
        // e.g. return value "_identity/api/users/test/resetpassword"
        return addRoutesPrefix(asList(new Route(POST, "/users/{name}/resetpassword")));
    }

}
