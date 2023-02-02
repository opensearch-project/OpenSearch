/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.add;

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
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Rest action for adding a permission to the permission store
 */
public class RestAddPermissionAction extends BaseRestHandler {

    @Override
    public String getName() {
        return RestConstants.IDENTITY_CREATE_PERMISSION_ACTION;
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        JsonNode contentAsNode;
        try {
            contentAsNode = DefaultObjectMapper.readTree(request.content().utf8ToString());
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(ErrorType.BODY_NOT_PARSEABLE.getMessage() + "Add Permission");
        }

        String permissionString = contentAsNode.get("permissionString").asText();
        String principalString = contentAsNode.get("principalString").asText();

        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
        addPermissionRequest.setPermissionString(permissionString);
        addPermissionRequest.setPrincipalString(principalString);
        request.withContentOrSourceParamParserOrNull((xContentParser -> {
            if (xContentParser != null) {
                try {
                    addPermissionRequest.fromXContent(xContentParser);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to parse add permission request body", e);
                }
            }
        }));

        // TODO: check if this bypass to directly doExecute is okay.
        // TODO: Ideally, this should be registered as `addPermission` request in Client.java and AbstractClient.java
        // TODO: see if you can add to RequestConverters.java to follow convention
        return channel -> client.doExecute(AddPermissionAction.INSTANCE, addPermissionRequest, new RestStatusToXContentListener<>(channel));
    }

    /**
     * Routes to be registered for this action
     * @return the unmodifiable list of routes to be registered
     */
    @Override
    public List<Route> routes() {
        // e.g. return value "_identity/api/internalusers/test"
        return addRoutesPrefix(asList(new Route(POST, "/permissions")));
    }
}
