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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Setting;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Contains all the method needed for a remote store backed node lifecycle.
 */
public class RemoteStoreService {

    private static final Logger logger = LogManager.getLogger(RemoteStoreService.class);
    private final Supplier<RepositoriesService> repositoriesService;
    private final ThreadPool threadPool;
    public static final Setting<CompatibilityMode> REMOTE_STORE_COMPATIBILITY_MODE_SETTING = new Setting<>(
        "remote_store.compatibility_mode",
        CompatibilityMode.STRICT.name(),
        CompatibilityMode::parseString,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Node join compatibility mode introduced with remote backed storage.
     *
     * @opensearch.internal
     */
    public enum CompatibilityMode {
        STRICT("strict"),
        ALLOW_MIX("allow_mix");

        public final String mode;

        CompatibilityMode(String mode) {
            this.mode = mode;
        }

        public static CompatibilityMode parseString(String compatibilityMode) {
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
    }

    public RemoteStoreService(Supplier<RepositoriesService> repositoriesService, ThreadPool threadPool) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    /**
     * Performs repository verification during node startup post its creation by invoking verify method against
     * repository mentioned. This verification will happen on a local node to validate if the node is able to connect
     * to the repository.
     */
    public void verifyRepositoriesLocally(List<Repository> repositories, DiscoveryNode localNode) {
        for (Repository repository : repositories) {
            String repositoryName = repository.getMetadata().name();
            String verificationToken = repository.startVerification();
            repository.verify(verificationToken, localNode);
            repository.endVerification(verificationToken);
            logger.info(() -> new ParameterizedMessage("successfully verified [{}] repository", repositoryName));
        }
    }

    /**
     * Creates a repository during a node startup.
     */
    public List<Repository> createRepositories(RemoteStoreNode node) {
        List<Repository> repositories = new ArrayList<>();
        for (RepositoryMetadata repositoryMetadata : node.getRepositoriesMetadata().repositories()) {
            RepositoriesService.validate(repositoryMetadata.name());
            Repository repository = repositoriesService.get().createRepository(repositoryMetadata);
            logger.info(
                "remote backed storage repository with name {} and type {} created.",
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

    /**
     * Updates repositories metadata in the cluster state if not already present. If a repository metadata for a
     * repository is already present in the cluster state and if it's different then the joining remote store backed
     * node repository metadata an exception will be thrown and the node will not be allowed to join the cluster.
     */
    public ClusterState updateClusterStateRepositoriesMetadata(RemoteStoreNode joiningNode, ClusterState currentState) {
        ClusterState newState = ClusterState.builder(currentState).build();
        for (RepositoryMetadata newRepositoryMetadata : joiningNode.getRepositoriesMetadata().repositories()) {
            newState = updateRepositoryMetadata(newRepositoryMetadata, newState);
        }
        return newState;
    }
}
