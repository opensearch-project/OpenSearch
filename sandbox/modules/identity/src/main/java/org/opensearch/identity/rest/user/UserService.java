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
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.rest.configuration.ConfigUpdateActionListener;
import org.opensearch.identity.User;
import org.opensearch.identity.configuration.CType;
import org.opensearch.identity.configuration.ConfigurationRepository;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.identity.exception.InvalidConfigException;
import org.opensearch.identity.exception.InvalidContentException;
import org.opensearch.identity.rest.user.create.CreateUserResponse;
import org.opensearch.identity.rest.user.create.CreateUserResponseInfo;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.identity.utils.Hasher;
import org.opensearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static org.apache.shiro.util.CollectionUtils.asList;

/**
 * Service layer class for handling User related functions for api requests
 */
public class UserService {
    private static final Logger logger = LogManager.getLogger(UserService.class);

    static final List<String> RESTRICTED_FROM_USERNAME = unmodifiableList(
        asList(
            ":" // Not allowed in basic auth, see https://stackoverflow.com/a/33391003/533057
        )
    );

    private final ClusterService clusterService;
    private final NodeClient nodeClient;

    private final ConfigurationRepository configurationRepository;

    private String identityIndex;

    @Inject
    public UserService(Settings settings, ClusterService clusterService, NodeClient nodeClient, ConfigurationRepository cr) {
        this.clusterService = clusterService;
        this.nodeClient = nodeClient;
        this.configurationRepository = cr;
        this.identityIndex = settings.get(ConfigConstants.IDENTITY_CONFIG_INDEX_NAME, ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX);
    }

    /**
     * Creates a user record in identity index (Updates if user already existed)
     * @param username of the user to be created
     * @param password of the user to be created (plain-text only)
     * @param listener on which the responses should be returned once execution completes
     */
    public void createUser(String username, String password, ActionListener<CreateUserResponse> listener) {

        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }

        // Username validation
        final List<String> foundRestrictedContents = RESTRICTED_FROM_USERNAME.stream()
            .filter(username::contains)
            .collect(Collectors.toList());
        if (!foundRestrictedContents.isEmpty()) {
            final String restrictedContents = foundRestrictedContents.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
            listener.onFailure(new InvalidContentException(ErrorType.RESTRICTED_CHARS_IN_USERNAME.getMessage() + restrictedContents));
            return;
        }

        // load current user store in memory
        final SecurityDynamicConfiguration<?> internalUsersConfiguration = load(getConfigName());

        // check if user existed
        final boolean userExisted = internalUsersConfiguration.exists(username);

        // TODO: should "existing user" case be handled via `PATCH` only ??

        // hash is mandatory for new users
        if (!userExisted && password == null) {
            listener.onFailure(new InvalidConfigException(ErrorType.HASH_OR_PASSWORD_MISSING.getMessage()));
            return;
        }

        User userToBeCreated = new User();
        // hash is optional for existing users
        if (userExisted && password == null) {
            // sanity check, this should usually not happen
            final String hash = ((User) internalUsersConfiguration.getCEntry(username)).getHash();
            userToBeCreated.setHash(hash);
        }

        // hash generated from provided plain-text password
        if (password != null) {
            // TODO: discuss if we are going to allow hash to be passed as request data instead of plain text password
            userToBeCreated.setHash(Hasher.hash(password.toCharArray()));
        }

        // TODO: check if this is absolutely required
        internalUsersConfiguration.remove(username);

        // Create or update the user
        internalUsersConfiguration.putCObject(username, userToBeCreated);

        // save the changes to identity index, propagate change to other nodes and reload in-memory configuration
        saveAndUpdateConfiguration(username, this.nodeClient, CType.INTERNALUSERS, internalUsersConfiguration, listener);
    }

    /**
     * Load data for a given CType
     * @param config CType whose data is to be loaded in-memory
     * @return configuration loaded with given CType data
     */
    protected final SecurityDynamicConfiguration<?> load(final CType config) {
        SecurityDynamicConfiguration<?> loaded = this.configurationRepository.getConfigurationsFromIndex(Collections.singleton(config))
            .get(config)
            .deepClone();
        return loaded;
    }

    protected CType getConfigName() {
        return CType.INTERNALUSERS;
    }

    /**
     * Check if identity index exists in cluster
     * @return true if exists, false otherwise
     */
    protected boolean ensureIndexExists() {
        if (!this.clusterService.state().metadata().hasConcreteIndex(this.identityIndex)) {
            return false;
        }
        return true;
    }

    /**
     * Persist changes to CType configuration in index, propagates this change to other nodes and reload in-memory cache
     * @param username of the user to be persisted in the index
     * @param client to execute index update request
     * @param cType Config Type to be reloaded
     * @param configuration Data to be persisted in index
     * @param listener on which to send response once execution completes
     */
    protected void saveAndUpdateConfiguration(
        final String username,
        final Client client,
        final CType cType,
        final SecurityDynamicConfiguration<?> configuration,
        ActionListener<CreateUserResponse> listener
    ) {
        // TODO: Future scope: see if this method can be generalized and extracted to another class

        // Listener for responding once index update completes
        final ActionListener<IndexResponse> actionListener = new OnSucessActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                CreateUserResponseInfo responseInfo = new CreateUserResponseInfo(true, username);
                CreateUserResponse response = new CreateUserResponse(unmodifiableList(asList(responseInfo)));

                listener.onResponse(response);
            }
        };
        final String id = cType.toLCString();

        try {
            // request to update the index
            final IndexRequest indexRequest = new IndexRequest(this.identityIndex);
            indexRequest.id(id)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setIfSeqNo(configuration.getSeqNo())
                .setIfPrimaryTerm(configuration.getPrimaryTerm())
                .source(id, XContentHelper.toXContent(configuration, XContentType.JSON, false));

            // writes to index and ConfigUpdateActionListener propagates change to other nodes by reloadConfiguration
            client.index(indexRequest, new ConfigUpdateActionListener<>(new String[] { id }, client, actionListener));
        } catch (IOException e) {
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
    }

    abstract class OnSucessActionListener<Response> implements ActionListener<Response> {

        public OnSucessActionListener() {
            super();
        }

        @Override
        public final void onFailure(Exception e) {
            // TODO throw it somewhere??
        }

    }
}
