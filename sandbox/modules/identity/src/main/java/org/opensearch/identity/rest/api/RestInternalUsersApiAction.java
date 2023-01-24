/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.identity.DefaultObjectMapper;
import org.opensearch.authn.Hashed;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.configuration.CType;
import org.opensearch.identity.configuration.ConfigurationRepository;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.identity.rest.validation.AbstractConfigurationValidator;
import org.opensearch.identity.rest.validation.InternalUsersValidator;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.threadpool.ThreadPool;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.identity.utils.RoutesHelper.addRoutesPrefix;

public class RestInternalUsersApiAction extends IdentityAbstractRestApiAction {
    static final List<String> RESTRICTED_FROM_USERNAME = unmodifiableList(
        asList(
            ":" // Not allowed in basic auth, see https://stackoverflow.com/a/33391003/533057
        )
    );

    private static final List<RestHandler.Route> routes = addRoutesPrefix(
        unmodifiableList(
            asList(
                new Route(Method.GET, "/user/{name}"),
                new Route(Method.GET, "/user/"),
                new Route(Method.DELETE, "/user/{name}"),
                new Route(Method.PUT, "/user/{name}"),

                // corrected mapping, introduced in OpenSearch Security
                new Route(Method.GET, "/internalusers/{name}"),
                new Route(Method.GET, "/internalusers/"),
                new Route(Method.DELETE, "/internalusers/{name}"),
                new Route(Method.PUT, "/internalusers/{name}"),
                new Route(Method.PATCH, "/internalusers/"),
                new Route(Method.PATCH, "/internalusers/{name}")
            )
        )
    );

    @Inject

    public RestInternalUsersApiAction(
        final Settings settings,
        final Path configPath,
        final RestController controller,
        final Client client,
        final ConfigurationRepository cl,
        final ClusterService cs,
        ThreadPool threadPool
    ) {
        super(settings, configPath, controller, client, cl, cs, threadPool);
    }

    @Override
    public List<Route> routes() {
        return routes;
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.INTERNALUSERS;
    }

    @Override
    protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content)
        throws IOException {

        final String username = request.param("name");

        if (username == null || username.length() == 0) {
            badRequestResponse(channel, "No " + getResourceName() + " specified.");
            return;
        }

        final List<String> foundRestrictedContents = RESTRICTED_FROM_USERNAME.stream()
            .filter(username::contains)
            .collect(Collectors.toList());
        if (!foundRestrictedContents.isEmpty()) {
            final String restrictedContents = foundRestrictedContents.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
            badRequestResponse(channel, "Username has restricted characters " + restrictedContents + " that are not permitted.");
            return;
        }

        // TODO it might be sensible to consolidate this with the overridden method in
        // order to minimize duplicated logic

        final SecurityDynamicConfiguration<?> internalUsersConfiguration = load(getConfigName());

        final ObjectNode contentAsNode = (ObjectNode) content;
        final JsonNode userContentAsNode = contentAsNode;

        // if password is set, it takes precedence over hash
        final String hashedPassword = userContentAsNode.get("hash").asText();

        final boolean userExisted = internalUsersConfiguration.exists(username);

        // when updating an existing user password hash can be blank, which means no
        // changes

        // sanity checks, hash is mandatory for newly created users
        if (!userExisted && hashedPassword == null) {
            badRequestResponse(channel, "Please specify either 'hash' or 'password' when creating a new internal user.");
            return;
        }

        // for existing users, hash is optional
        if (userExisted && hashedPassword == null) {
            // sanity check, this should usually not happen
            final String hash = ((Hashed) internalUsersConfiguration.getCEntry(username)).getHash();
            if (hash == null || hash.length() == 0) {
                internalErrorResponse(
                    channel,
                    "Existing user " + username + " has no password, and no new password or hash was specified."
                );
                return;
            }
            contentAsNode.put("hash", hash);
        }

        internalUsersConfiguration.remove(username);

        // checks complete, create or update the user
        internalUsersConfiguration.putCObject(
            username,
            DefaultObjectMapper.readTree(contentAsNode, internalUsersConfiguration.getImplementingClass())
        );

        saveAndUpdateConfigs(
            client,
            request,
            CType.INTERNALUSERS,
            internalUsersConfiguration,
            new OnSucessActionListener<IndexResponse>(channel) {

                @Override
                public void onResponse(IndexResponse response) {
                    if (userExisted) {
                        successResponse(channel, "'" + username + "' updated.");
                    } else {
                        createdResponse(channel, "'" + username + "' created.");
                    }

                }
            }
        );
    }

    @Override
    protected String getResourceName() {
        return "user";
    }

    @Override
    protected CType getConfigName() {
        return CType.INTERNALUSERS;
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... params) {
        return new InternalUsersValidator(request, isSuperAdmin(), ref, this.settings, params);
    }
}
