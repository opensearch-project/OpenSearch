/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.identity.rest.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.authz.OpenSearchPermission;
import org.opensearch.identity.rest.action.permission.add.AddPermissionResponse;
import org.opensearch.identity.rest.action.permission.check.CheckPermissionResponse;
import org.opensearch.identity.rest.action.permission.delete.DeletePermissionResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.transport.TransportService;
import org.opensearch.identity.utils.ErrorType;

import java.util.List;

/**
 * Service class for Permission related functions
 */
public class PermissionService {

    private static final Logger logger = LogManager.getLogger(PermissionService.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final NodeClient nodeClient;

    private final String identityIndex = ConfigConstants.IDENTITY_CONFIG_INDEX_NAME;

    @Inject
    public PermissionService(ClusterService clusterService, TransportService transportService, NodeClient nodeClient) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.nodeClient = nodeClient;
    }

    public void addPermission(String principal, String permissionString, ActionListener<AddPermissionResponse> listener) {

        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }

        OpenSearchPermission permissionToBeAdded = new OpenSearchPermission(permissionString);
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
        internalUsersConfiguration.putCObject(username, userToBeCreated);

        // save the changes to identity index
        saveAndUpdateConfiguration(username, this.nodeClient, CType.INTERNALUSERS, internalUsersConfiguration, listener);
    }

    protected final SecurityDynamicConfiguration<?> load(final CType config) {
        SecurityDynamicConfiguration<?> loaded = this.configurationRepository.getConfigurationsFromIndex(Collections.singleton(config))
            .get(config)
            .deepClone();
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
        final String username,
        final Client client,
        final CType cType,
        final SecurityDynamicConfiguration<?> configuration,
        ActionListener<CreateUserResponse> listener
    ) {
        final ActionListener<IndexResponse> actionListener = new OnSucessActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                CreateUserResponseInfo responseInfo = new CreateUserResponseInfo(true, username);
                CreateUserResponse response = new CreateUserResponse(unmodifiableList(asList(responseInfo)));

                listener.onResponse(response);
            }
        };
        final IndexRequest ir = new IndexRequest(this.identityIndex);

        final String id = cType.toLCString();

        try {
            client.index(
                ir.id(id)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setIfSeqNo(configuration.getSeqNo())
                    .setIfPrimaryTerm(configuration.getPrimaryTerm())
                    .source(id, XContentHelper.toXContent(configuration, XContentType.JSON, false)),
                new ConfigUpdateActionListener<>(new String[] { id }, client, actionListener)
            );

            // execute it at transport level
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
    }

    public void checkPermission(String username, ActionListener<CheckPermissionResponse> listener) {
        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }
    }

    public void deletePermission(String username, String password, ActionListener<DeletePermissionResponse> listener) {
        if (!ensureIndexExists()) {
            listener.onFailure(new IndexNotFoundException(ErrorType.IDENTITY_NOT_INITIALIZED.getMessage()));
            return;
        }
    }


}
