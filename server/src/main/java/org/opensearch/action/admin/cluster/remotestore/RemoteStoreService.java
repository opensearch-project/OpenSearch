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
import org.opensearch.repositories.RepositoryVerificationException;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Contains all the method needed for a remote store node lifecycle.
 */
public class RemoteStoreService {

    private static final Logger logger = LogManager.getLogger(RemoteStoreService.class);
    private final Supplier<RepositoriesService> repositoriesService;
    private final ThreadPool threadPool;
    public static final Setting<String> REMOTE_STORE_COMPATIBILITY_MODE_SETTING = Setting.simpleString(
        "remote_store.compatibility_mode",
        CompatibilityMode.ALLOW_ONLY_REMOTE_STORE_NODES.value,
        CompatibilityMode::validate,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public enum CompatibilityMode {
        ALLOW_ONLY_REMOTE_STORE_NODES("allow_only_remote_store_nodes"),
        ALLOW_ALL_NODES("allow_all_nodes");

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

    public RemoteStoreService(Supplier<RepositoriesService> repositoriesService, ThreadPool threadPool) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    public void verifyRepository(List<Repository> repositories, DiscoveryNode localNode) {
        for (Repository repository : repositories) {
            String verificationToken = repository.startVerification();
            ExecutorService executor = threadPool.executor(ThreadPool.Names.GENERIC);
            executor.execute(() -> {
                try {
                    repository.verify(verificationToken, localNode);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to verify repository", repository), e);
                    throw new RepositoryVerificationException(repository.getMetadata().name(), e.getMessage());
                }
            });

            // TODO: See if using listener here which is async makes sense, made this sync as
            //  we need the repository registration for remote store node to be completed before the bootstrap
            //  completes.
            try {
                if(executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    throw new RepositoryVerificationException(repository.getMetadata().name(), "could not complete " +
                        "repository verification within timeout.");
                }
            } catch (InterruptedException e) {
                throw new RepositoryVerificationException(repository.getMetadata().name(), e.getMessage());
            }
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
