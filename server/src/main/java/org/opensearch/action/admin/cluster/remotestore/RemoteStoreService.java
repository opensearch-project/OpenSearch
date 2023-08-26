/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Contains all the method needed for a remote store node lifecycle.
 */
public class RemoteStoreService {

    private static final Logger logger = LogManager.getLogger(RemoteStoreService.class);
    private final Supplier<RepositoriesService> repositoriesService;
    public static final Setting<String> REMOTE_STORE_MIGRATION_SETTING = Setting.simpleString(
        "remote_store.migration",
        MigrationTypes.NOT_MIGRATING.value,
        MigrationTypes::validate,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public enum MigrationTypes {
        NOT_MIGRATING("not_migrating"),
        MIGRATING_TO_REMOTE_STORE("migrating_to_remote_store"),
        MIGRATING_TO_HOT("migrating_to_hot");

        public static MigrationTypes validate(String migrationType) {
            try {
                return MigrationTypes.valueOf(migrationType.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "["
                        + migrationType
                        + "] migration type is not supported. "
                        + "Supported migration types are ["
                        + MigrationTypes.values().toString()
                        + "]"
                );
            }
        }

        public final String value;

        MigrationTypes(String value) {
            this.value = value;
        }
    }

    public RemoteStoreService(Supplier<RepositoriesService> repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public void verifyRepository(RepositoryMetadata repositoryMetadata) {
        ActionListener<VerifyRepositoryResponse> listener = new ActionListener<>() {

            @Override
            public void onResponse(VerifyRepositoryResponse verifyRepositoryResponse) {
                logger.info("Successfully verified repository : " + verifyRepositoryResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                throw new IllegalStateException("Failed to finish remote store repository verification" + e.getMessage());
            }
        };

        repositoriesService.get()
            .verifyRepository(
                repositoryMetadata.name(),
                ActionListener.delegateFailure(
                    listener,
                    (delegatedListener, verifyResponse) -> delegatedListener.onResponse(
                        new VerifyRepositoryResponse(verifyResponse.toArray(new DiscoveryNode[0]))
                    )
                )
            );
    }

    private ClusterState createRepository(RepositoryMetadata newRepositoryMetadata, ClusterState currentState) {
        RepositoriesService.validate(newRepositoryMetadata.name());

        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
        if (repositories == null) {
            Repository repository = repositoriesService.get().createRepository(newRepositoryMetadata);
            logger.info(
                "Remote store repository with name {} and type {} created.",
                repository.getMetadata().name(),
                repository.getMetadata().type()
            );
            repositories = new RepositoriesMetadata(Collections.singletonList(newRepositoryMetadata));
        } else {
            List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);

            for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                    if (newRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata)) {
                        return new ClusterState.Builder(currentState).build();
                    } else {
                        throw new IllegalStateException(
                            "new repository metadata ["
                                + newRepositoryMetadata
                                + "] supplied by joining node is different from existing repository metadata ["
                                + repositoryMetadata
                                + "]."
                        );
                    }
                } else {
                    repositoriesMetadata.add(repositoryMetadata);
                }
            }
            Repository repository = repositoriesService.get().createRepository(newRepositoryMetadata);
            logger.info(
                "Remote store repository with name {} and type {} created",
                repository.getMetadata().name(),
                repository.getMetadata().type()
            );
            repositoriesMetadata.add(newRepositoryMetadata);
            repositories = new RepositoriesMetadata(repositoriesMetadata);
        }
        mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private boolean isRepositoryCreated(RepositoryMetadata repositoryMetadata) {
        try {
            repositoriesService.get().repository(repositoryMetadata.name());
            return true;
        } catch (RepositoryMissingException e) {
            return false;
        }
    }

    private boolean isRepositoryAddedInClusterState(RepositoryMetadata repositoryMetadata, ClusterState currentState) {
        RepositoriesMetadata repositoriesMetadata = currentState.metadata().custom(RepositoriesMetadata.TYPE);
        if (repositoriesMetadata == null) {
            return false;
        }
        for (RepositoryMetadata existingRepositoryMetadata : repositoriesMetadata.repositories()) {
            existingRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata);
            return true;
        }
        return false;
    }

    private ClusterState createOrVerifyRepository(RepositoriesMetadata repositoriesMetadata, ClusterState currentState) {
        ClusterState newState = ClusterState.builder(currentState).build();
        for (RepositoryMetadata repositoryMetadata : repositoriesMetadata.repositories()) {
            if (isRepositoryCreated(repositoryMetadata)) {
                verifyRepository(repositoryMetadata);
            } else {
                if (!isRepositoryAddedInClusterState(repositoryMetadata, currentState)) {
                    newState = ClusterState.builder(createRepository(repositoryMetadata, newState)).build();
                    // verifyRepository(repositoryMetadata);
                }
            }
        }
        return newState;
    }

    public ClusterState joinCluster(RemoteStoreNode joiningRemoteStoreNode, ClusterState currentState) {
        List<DiscoveryNode> existingNodes = new ArrayList<>(currentState.nodes().getNodes().values());
        if (existingNodes.isEmpty()) {
            return currentState;
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        if (existingNodes.get(0).isRemoteStoreNode()) {
            RemoteStoreNode existingRemoteStoreNode = new RemoteStoreNode(existingNodes.get(0));
            if (existingRemoteStoreNode.equals(joiningRemoteStoreNode)) {
                newState = ClusterState.builder(createOrVerifyRepository(joiningRemoteStoreNode.getRepositoriesMetadata(), currentState));
            }
        } else {
            throw new IllegalStateException(
                "a remote store node [" + joiningRemoteStoreNode + "] is trying to join a non remote store cluster."
            );
        }
        return newState.build();
    }
}
