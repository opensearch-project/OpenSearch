/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.authn.StringPrincipal;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.ConfigUpdatingActionListener;
import org.opensearch.identity.User;
import org.opensearch.identity.configuration.CType;
import org.opensearch.identity.configuration.ConfigurationRepository;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.identity.exception.InvalidConfigException;
import org.opensearch.identity.exception.InvalidUserNameException;
import org.opensearch.identity.rest.user.create.CreateUserResponse;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static org.apache.shiro.util.CollectionUtils.asList;

/**
 * Service class for User related functions
 */
public class UserService {
    private static final Logger logger = LogManager.getLogger(UserService.class);

    static final List<String> RESTRICTED_FROM_USERNAME = unmodifiableList(
        asList(
            ":" // Not allowed in basic auth, see https://stackoverflow.com/a/33391003/533057
        )
    );

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final NodeClient nodeClient;

    private final ConfigurationRepository configurationRepository;


    private final String identityIndex = ConfigConstants.IDENTITY_CONFIG_INDEX_NAME;

    @Inject
    public UserService(ClusterService clusterService, TransportService transportService, NodeClient nodeClient, ConfigurationRepository cr) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.nodeClient = nodeClient;
        this.configurationRepository = cr;
    }

    public void createUser(String username, String password, ActionListener<CreateUserResponse> listener) {
        // TODO: Implement this

        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }

        final List<String> foundRestrictedContents = RESTRICTED_FROM_USERNAME.stream()
            .filter(username::contains)
            .collect(Collectors.toList());
        if (!foundRestrictedContents.isEmpty()) {
            final String restrictedContents = foundRestrictedContents.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
            listener.onFailure(new InvalidUserNameException(ErrorType.RESTRICTED_CHARS_IN_USERNAME.getMessage() + restrictedContents));
            return;
        }

        User userToBeCreated = new User();
        userToBeCreated.setUsername(new StringPrincipal(username));

        final SecurityDynamicConfiguration<?> internalUsersConfiguration = load(getConfigName());

        final boolean userExisted = internalUsersConfiguration.exists(username);

        // sanity checks, hash is mandatory for newly created users
        if (!userExisted && password == null) {
            listener.onFailure(new InvalidConfigException(ErrorType.HASH_OR_PASSWORD_MISSING.getMessage()));
            return;
        }

        // for existing users, hash is optional
        if (userExisted && password == null) {
            // sanity check, this should usually not happen
            final String hash = ((User) internalUsersConfiguration.getCEntry(username)).getHash();
            userToBeCreated.setHash(hash);
        }

        internalUsersConfiguration.remove(username);

        // checks complete, create or update the user
        internalUsersConfiguration.putCObject(
            username,
            userToBeCreated
        );

        saveAndUpdateConfiguration(
            this.nodeClient,
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

    protected final SecurityDynamicConfiguration<?> load(final CType config) {
        SecurityDynamicConfiguration<?> loaded = this.configurationRepository.getConfigurationsFromIndex(Collections.singleton(config)).get(config).deepClone();
        return loaded;
    }

    protected CType getConfigName() {
        return CType.INTERNALUSERS;
    }

    protected boolean ensureIndexExists() {
        if (!this.clusterService.state().metadata().hasConcreteIndex(this.identityIndex)) {
            return false;
        }
        return true;
    }


    protected void saveAndUpdateConfiguration(
        final Client client,
        final CType cType,
        final SecurityDynamicConfiguration<?> configuration,
        OnSucessActionListener<IndexResponse> actionListener
    ) {
        final IndexRequest ir = new IndexRequest(this.identityIndex);

        final String id = cType.toLCString();

        try {
            client.index(
                ir.id(id)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIfSeqNo(configuration.getSeqNo())
                    .setIfPrimaryTerm(configuration.getPrimaryTerm())
                    .source(id, XContentHelper.toXContent(configuration, XContentType.JSON, false)),
                new ConfigUpdatingActionListener<>(new String[] { id }, client, actionListener)
            );

            // execute it at transport level
        } catch (IOException e) {
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }
}
