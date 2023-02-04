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
import org.opensearch.ResourceNotFoundException;
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
import org.opensearch.identity.IdentityConfigConstants;
import org.opensearch.identity.rest.configuration.IdentityConfigUpdateActionListener;
import org.opensearch.identity.User;
import org.opensearch.identity.configuration.CType;
import org.opensearch.identity.configuration.ConfigurationRepository;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.identity.exception.InvalidConfigException;
import org.opensearch.identity.exception.InvalidContentException;
import org.opensearch.identity.rest.user.delete.DeleteUserResponse;
import org.opensearch.identity.rest.user.delete.DeleteUserResponseInfo;
import org.opensearch.identity.rest.user.get.multi.MultiGetUserResponse;
import org.opensearch.identity.rest.user.get.single.GetUserResponse;
import org.opensearch.identity.rest.user.get.single.GetUserResponseInfo;
import org.opensearch.identity.rest.user.put.PutUserResponse;
import org.opensearch.identity.rest.user.put.PutUserResponseInfo;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.identity.utils.Hasher;
import org.opensearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        this.identityIndex = settings.get(
            IdentityConfigConstants.IDENTITY_CONFIG_INDEX_NAME,
            IdentityConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX
        );
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
     * Creates or updates a user record in identity index (Updates if user already existed)
     * @param userToBeCreated user object to be created
     * @param listener on which the responses should be returned once execution completes
     */
    public void createOrUpdateUser(User userToBeCreated, ActionListener<PutUserResponse> listener) {

        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }

        String username = userToBeCreated.getUsername().getName();
        String password = userToBeCreated.getHash();

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

        // hash is mandatory for new users
        if (!userExisted && password == null) {
            listener.onFailure(new InvalidConfigException(ErrorType.HASH_OR_PASSWORD_MISSING.getMessage()));
            return;
        }

        User existingUser = null;

        // update permissions
        Set<String> permissions = new HashSet<>();

        // hash is optional for existing users
        if (userExisted && password == null) {
            // sanity check, this should usually not happen
            final String hash = ((User) internalUsersConfiguration.getCEntry(username)).getHash();
            userToBeCreated.setHash(hash);
        }

        // TODO: revisit this logic:
        // 1. Are we going to create a user with permissions
        // 2. Are we going to allow addition of new permissions to existing user (should be done via `permissions` API)
        if (userExisted) {
            existingUser = (User) internalUsersConfiguration.getCEntry(username);
            // add current permissions
            Optional.ofNullable(existingUser.getPermissions()).ifPresent(permissions::addAll);
        } else {
            // add new permissions
            Optional.ofNullable(userToBeCreated.getPermissions()).ifPresent(permissions::addAll);
        }

        userToBeCreated.setPermissions(permissions);

        // hash generated from provided plain-text password
        if (password != null) {
            // TODO: discuss if we are going to allow hash to be passed as request data instead of plain text password
            userToBeCreated.setHash(Hasher.hash(password.toCharArray()));
        }

        // this is needed, otherwise jackson databind throws StringPrincipal related errors
        userToBeCreated.setUsername(null);

        // TODO: check if this is absolutely required
        internalUsersConfiguration.remove(username);

        // Create or update the user
        internalUsersConfiguration.putCObject(username, userToBeCreated);

        // Listener for responding once index update completes
        final ActionListener<IndexResponse> indexActionListener = new OnSucessActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                String operationResponse = userExisted ? " updated successfully." : " created successfully.";
                String message = username + operationResponse;
                PutUserResponseInfo responseInfo = new PutUserResponseInfo(true, username, message);
                PutUserResponse response = new PutUserResponse(responseInfo);

                listener.onResponse(response);
            }
        };

        // save the changes to identity index, propagate change to other nodes and reload in-memory configuration
        saveAndUpdateConfiguration(this.nodeClient, CType.INTERNALUSERS, internalUsersConfiguration, indexActionListener);
    }

    public void getUser(String username, ActionListener<GetUserResponse> listener) {
        if (username == null || username == "") {
            listener.onResponse(new GetUserResponse());
            return;
        }

        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }

        // load current user store in memory
        final SecurityDynamicConfiguration<?> internalUsersConfiguration = load(getConfigName());

        // check if user existed
        final boolean userExists = internalUsersConfiguration.exists(username);

        // hash is optional for existing users
        if (!userExists) {
            listener.onFailure(new ResourceNotFoundException(username + ErrorType.RESOURCE_NOT_FOUND_SUFFIX.getMessage()));
            return;
        }

        // Return this user
        User user = (User) internalUsersConfiguration.getCEntry(username);

        // formulate response to be returned
        GetUserResponseInfo responseInfo = new GetUserResponseInfo(username, user.getAttributes(), user.getPermissions());
        GetUserResponse response = new GetUserResponse(responseInfo);

        // success response
        listener.onResponse(response);
    }

    @SuppressWarnings("unchecked")
    public void getUsers(ActionListener<MultiGetUserResponse> listener) {

        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }

        // load current user store in memory
        final SecurityDynamicConfiguration<?> internalUsersConfiguration = load(getConfigName());

        final Map<String, User> users = (Map<String, User>) internalUsersConfiguration.getCEntries();

        List<GetUserResponseInfo> usersInfo = new ArrayList<>();

        users.forEach((name, user) -> { usersInfo.add(new GetUserResponseInfo(name, user.getAttributes(), user.getPermissions())); });

        // formulate response to be returned
        MultiGetUserResponse response = new MultiGetUserResponse(usersInfo);

        // success response
        listener.onResponse(response);
    }

    public void deleteUser(String username, ActionListener<DeleteUserResponse> listener) {
        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }

        // load current user store in memory
        final SecurityDynamicConfiguration<?> internalUsersConfiguration = load(getConfigName());

        // check if user existed
        final boolean userExisted = internalUsersConfiguration.exists(username);

        // hash is mandatory for new users
        if (!userExisted) {
            listener.onFailure(new ResourceNotFoundException(username + ErrorType.RESOURCE_NOT_FOUND_SUFFIX.getMessage()));
            return;
        }

        // Delete the user
        internalUsersConfiguration.remove(username);

        // Listener for responding once index update completes
        final ActionListener<IndexResponse> indexActionListener = new OnSucessActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                String message = username + " deleted successfully.";
                DeleteUserResponseInfo responseInfo = new DeleteUserResponseInfo(true, message);
                DeleteUserResponse response = new DeleteUserResponse(unmodifiableList(asList(responseInfo)));

                listener.onResponse(response);
            }
        };

        // save the changes to identity index, propagate change to other nodes and reload in-memory configuration
        saveAndUpdateConfiguration(this.nodeClient, CType.INTERNALUSERS, internalUsersConfiguration, indexActionListener);
    }

    /**
     * Persist changes to CType configuration in index, propagates this change to other nodes and reload in-memory cache
     * @param client              to execute index update request
     * @param cType               Config Type to be reloaded
     * @param configuration       Data to be persisted in index
     * @param indexActionListener On which to send response once index request execution completes
     */
    protected void saveAndUpdateConfiguration(
        final Client client,
        final CType cType,
        final SecurityDynamicConfiguration<?> configuration,
        ActionListener<IndexResponse> indexActionListener
    ) {
        // TODO: Future scope: see if this method can be generalized and extracted to another class

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
            client.index(indexRequest, new IdentityConfigUpdateActionListener<>(new String[] { id }, client, indexActionListener));
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
