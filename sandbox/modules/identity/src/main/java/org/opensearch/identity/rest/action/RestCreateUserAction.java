/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action;

import org.opensearch.client.node.NodeClient;
import org.opensearch.identity.rest.request.CreateUserRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.identity.utils.RoutesHelper.addRoutesPrefix;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action for creating user
 */
public class RestCreateUserAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "create_user_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String username = request.param("name");
        String password = request.param("password");
        CreateUserRequest createUserRequest = new CreateUserRequest(username, password);

        // TODO: check if this bypass to directly doExecute is okay.
        // TODO: Ideally, this should be registered as `createUser` request in Client.java and AbstractClient.java
        // TODO: see if you can add to RequestConverters.java to follow convention
        return channel -> client.doExecute(CreateUserAction.INSTANCE, createUserRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return addRoutesPrefix(
            unmodifiableList(
                asList(
                    new Route(PUT, "/internalusers/{name}")
                )
            ));
    }

}

