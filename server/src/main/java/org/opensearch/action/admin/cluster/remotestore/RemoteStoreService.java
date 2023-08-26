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
    public static final Setting<String> REMOTE_STORE_COMPATIBILITY_MODE_SETTING = Setting.simpleString(
        "remote_store.compatibility_mode",
        CompatibilityMode.ALLOW_ONLY_REMOTE_STORE_NODES.value,
        CompatibilityMode::validate,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public enum CompatibilityMode {
        ALLOW_ONLY_REMOTE_STORE_NODES("allow_only_remote_store_nodes"),
        ALLOW_ALL_NODES("allow_all_nodes"),
        MIGRATING_TO_HOT("migrating_to_hot");

        public static CompatibilityMode validate(String compatibilityMode) {
            try {
                return CompatibilityMode.valueOf(compatibilityMode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "["
                        + compatibilityMode
                        + "] compatibility mode is not supported. "
                        + "supported modes are ["
                        + CompatibilityMode.values().toString()
                        + "]"
                );
            }
        }

        public final String value;

        CompatibilityMode(String value) {
            this.value = value;
        }
    }

    public RemoteStoreService(Supplier<RepositoriesService> repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public void verifyRepository(List<Repository> repositories) {
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

        for (Repository repository : repositories) {
            repositoriesService.get()
                .verifyRepository(
                    repository.getMetadata().name(),
                    ActionListener.delegateFailure(
                        listener,
                        (delegatedListener, verifyResponse) -> delegatedListener.onResponse(
                            new VerifyRepositoryResponse(verifyResponse.toArray(new DiscoveryNode[0]))
                        )
                    )
                );
        }
    }

    public List<Repository> createRepositories(RemoteStoreNode node) {
        List<Repository> repositories = new ArrayList<>();
        for (RepositoryMetadata repositoryMetadata : node.getRepositoriesMetadata().repositories()) {
            RepositoriesService.validate(repositoryMetadata.name());
            Repository repository = repositoriesService.get().createRepository(repositoryMetadata);
            logger.info(
                "Remote store repository with name {} and type {} created.",
                repository.getMetadata().name(),
                repository.getMetadata().type()
            );
            repositories.add(repository);
        }
        return repositories;
    }

    private ClusterState updateRepositoryMetadata(RepositoryMetadata newRepositoryMetadata, ClusterState currentState) {
        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
        if (repositories == null) {
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
            repositoriesMetadata.add(newRepositoryMetadata);
            repositories = new RepositoriesMetadata(repositoriesMetadata);
        }
        mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    public ClusterState updateClusterStateRepositoriesMetadata(RemoteStoreNode node, ClusterState currentState) {
        ClusterState newState = ClusterState.builder(currentState).build();
        for (RepositoryMetadata newRepositoryMetadata : node.getRepositoriesMetadata().repositories()) {
            newState = updateRepositoryMetadata(newRepositoryMetadata, newState);
        }
        return newState;
    }
}
