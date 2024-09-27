/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.crypto.CryptoSettings;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.RepositoryCleanupInProgress;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.opensearch.snapshots.SnapshotsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.repositories.blobstore.BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SHALLOW_SNAPSHOT_V2;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RepositoriesService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RepositoriesService.class);

    public static final Setting<TimeValue> REPOSITORIES_STATS_ARCHIVE_RETENTION_PERIOD = Setting.positiveTimeSetting(
        "repositories.stats.archive.retention_period",
        TimeValue.timeValueHours(2),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> REPOSITORIES_STATS_ARCHIVE_MAX_ARCHIVED_STATS = Setting.intSetting(
        "repositories.stats.archive.max_archived_stats",
        100,
        0,
        Setting.Property.NodeScope
    );

    private final Map<String, Repository.Factory> typesRegistry;
    private final Map<String, Repository.Factory> internalTypesRegistry;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final VerifyNodeRepositoryAction verifyAction;

    private final Map<String, Repository> internalRepositories = ConcurrentCollections.newConcurrentMap();
    private volatile Map<String, Repository> repositories = Collections.emptyMap();
    private final RepositoriesStatsArchive repositoriesStatsArchive;
    private final ClusterManagerTaskThrottler.ThrottlingKey putRepositoryTaskKey;
    private final ClusterManagerTaskThrottler.ThrottlingKey deleteRepositoryTaskKey;
    private final Settings settings;

    public RepositoriesService(
        Settings settings,
        ClusterService clusterService,
        TransportService transportService,
        Map<String, Repository.Factory> typesRegistry,
        Map<String, Repository.Factory> internalTypesRegistry,
        ThreadPool threadPool
    ) {
        this.settings = settings;
        this.typesRegistry = typesRegistry;
        this.internalTypesRegistry = internalTypesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isClusterManagerNode(settings)) {
            if (isDedicatedVotingOnlyNode(DiscoveryNode.getRolesFromSettings(settings)) == false) {
                clusterService.addHighPriorityApplier(this);
            }
        }
        this.verifyAction = new VerifyNodeRepositoryAction(transportService, clusterService, this);
        this.repositoriesStatsArchive = new RepositoriesStatsArchive(
            REPOSITORIES_STATS_ARCHIVE_RETENTION_PERIOD.get(settings),
            REPOSITORIES_STATS_ARCHIVE_MAX_ARCHIVED_STATS.get(settings),
            threadPool::relativeTimeInMillis
        );
        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        putRepositoryTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.PUT_REPOSITORY_KEY, true);
        deleteRepositoryTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.DELETE_REPOSITORY_KEY, true);
    }

    /**
     * Registers new repository or updates an existing repository in the cluster
     * <p>
     * This method can be only called on the cluster-manager node. It tries to create a new repository on the master
     * and if it was successful it adds new repository to cluster metadata.
     *
     * @param request  register repository request
     * @param listener register repository listener
     */
    public void registerOrUpdateRepository(final PutRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";

        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(
            request.name(),
            request.type(),
            request.settings(),
            CryptoMetadata.fromRequest(request.cryptoSettings())
        );
        validate(request.name());
        validateRepositoryMetadataSettings(clusterService, request.name(), request.settings(), repositories, settings, this);
        if (newRepositoryMetadata.cryptoMetadata() != null) {
            validate(newRepositoryMetadata.cryptoMetadata().keyProviderName());
        }

        final ActionListener<ClusterStateUpdateResponse> registrationListener;
        if (request.verify()) {
            registrationListener = ActionListener.delegateFailure(listener, (delegatedListener, clusterStateUpdateResponse) -> {
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    // The response was acknowledged - all nodes should know about the new repository, let's verify them
                    verifyRepository(
                        request.name(),
                        ActionListener.delegateFailure(
                            delegatedListener,
                            (innerDelegatedListener, discoveryNodes) -> innerDelegatedListener.onResponse(clusterStateUpdateResponse)
                        )
                    );
                } else {
                    delegatedListener.onResponse(clusterStateUpdateResponse);
                }
            });
        } else {
            registrationListener = listener;
        }

        // Trying to create the new repository on cluster-manager to make sure it works
        try {
            closeRepository(createRepository(newRepositoryMetadata, typesRegistry));
        } catch (Exception e) {
            registrationListener.onFailure(e);
            return;
        }

        clusterService.submitStateUpdateTask(
            "put_repository [" + request.name() + "]",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, registrationListener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    ensureRepositoryNotInUse(currentState, request.name());
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
                    if (repositories == null) {
                        logger.info("put repository [{}]", request.name());
                        repositories = new RepositoriesMetadata(
                            Collections.singletonList(
                                new RepositoryMetadata(
                                    request.name(),
                                    request.type(),
                                    request.settings(),
                                    CryptoMetadata.fromRequest(request.cryptoSettings())
                                )
                            )
                        );
                    } else {
                        boolean found = false;
                        List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);

                        for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                            RepositoryMetadata updatedRepositoryMetadata = newRepositoryMetadata;
                            if (isSystemRepositorySettingPresent(repositoryMetadata.settings())) {
                                Settings updatedSettings = Settings.builder()
                                    .put(newRepositoryMetadata.settings())
                                    .put(SYSTEM_REPOSITORY_SETTING.getKey(), true)
                                    .build();
                                updatedRepositoryMetadata = new RepositoryMetadata(
                                    newRepositoryMetadata.name(),
                                    newRepositoryMetadata.type(),
                                    updatedSettings,
                                    newRepositoryMetadata.cryptoMetadata()
                                );
                            }
                            if (repositoryMetadata.name().equals(updatedRepositoryMetadata.name())) {
                                if (updatedRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata)) {
                                    // Previous version is the same as this one no update is needed.
                                    return currentState;
                                }
                                ensureCryptoSettingsAreSame(repositoryMetadata, request);
                                found = true;
                                if (isSystemRepositorySettingPresent(repositoryMetadata.settings())) {
                                    ensureValidSystemRepositoryUpdate(updatedRepositoryMetadata, repositoryMetadata);
                                }
                                repositoriesMetadata.add(updatedRepositoryMetadata);
                            } else {
                                repositoriesMetadata.add(repositoryMetadata);
                            }
                        }
                        if (!found) {
                            logger.info("put repository [{}]", request.name());
                            repositoriesMetadata.add(
                                new RepositoryMetadata(
                                    request.name(),
                                    request.type(),
                                    request.settings(),
                                    CryptoMetadata.fromRequest(request.cryptoSettings())
                                )
                            );
                        } else {
                            logger.info("update repository [{}]", request.name());
                        }
                        repositories = new RepositoriesMetadata(repositoriesMetadata);
                    }
                    mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return putRepositoryTaskKey;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name()), e);
                    super.onFailure(source, e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository is created on both cluster-manager and data nodes
                    return discoveryNode.isClusterManagerNode() || discoveryNode.isDataNode();
                }
            }
        );
    }

    /**
     * Unregisters repository in the cluster
     * <p>
     * This method can be only called on the cluster-manager node. It removes repository information from cluster metadata.
     *
     * @param request  unregister repository request
     * @param listener unregister repository listener
     */
    public void unregisterRepository(final DeleteRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
            "delete_repository [" + request.name() + "]",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
                    if (repositories != null && repositories.repositories().size() > 0) {
                        List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size());
                        boolean changed = false;
                        for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                            if (Regex.simpleMatch(request.name(), repositoryMetadata.name())) {
                                ensureRepositoryNotInUse(currentState, repositoryMetadata.name());
                                ensureNotSystemRepository(repositoryMetadata);
                                logger.info("delete repository [{}]", repositoryMetadata.name());
                                changed = true;
                            } else {
                                repositoriesMetadata.add(repositoryMetadata);
                            }
                        }
                        if (changed) {
                            repositories = new RepositoriesMetadata(repositoriesMetadata);
                            mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                            return ClusterState.builder(currentState).metadata(mdBuilder).build();
                        }
                    }
                    if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                        return currentState;
                    }
                    throw new RepositoryMissingException(request.name());
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return deleteRepositoryTaskKey;
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository was created on both cluster-manager and data nodes
                    return discoveryNode.isClusterManagerNode() || discoveryNode.isDataNode();
                }
            }
        );
    }

    public void verifyRepository(final String repositoryName, final ActionListener<List<DiscoveryNode>> listener) {
        final Repository repository = repository(repositoryName);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<List<DiscoveryNode>>(listener) {
            @Override
            protected void doRun() {
                final String verificationToken = repository.startVerification();
                if (verificationToken != null) {
                    try {
                        verifyAction.verify(
                            repositoryName,
                            verificationToken,
                            ActionListener.delegateFailure(
                                listener,
                                (delegatedListener, verifyResponse) -> threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                    try {
                                        repository.endVerification(verificationToken);
                                    } catch (Exception e) {
                                        logger.warn(
                                            () -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName),
                                            e
                                        );
                                        delegatedListener.onFailure(e);
                                        return;
                                    }
                                    delegatedListener.onResponse(verifyResponse);
                                })
                            )
                        );
                    } catch (Exception e) {
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                            try {
                                repository.endVerification(verificationToken);
                            } catch (Exception inner) {
                                inner.addSuppressed(e);
                                logger.warn(
                                    () -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName),
                                    inner
                                );
                            }
                            listener.onFailure(e);
                        });
                    }
                } else {
                    listener.onResponse(Collections.emptyList());
                }
            }
        });
    }

    // Note: "voting_only" role has not been implemented yet, so the method always returns false.
    static boolean isDedicatedVotingOnlyNode(Set<DiscoveryNodeRole> roles) {
        return roles.contains(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)
            && roles.contains(DiscoveryNodeRole.DATA_ROLE) == false
            && roles.stream().anyMatch(role -> role.roleName().equals("voting_only"));
    }

    /**
     * Checks if new repositories appeared in or disappeared from cluster metadata and updates current list of
     * repositories accordingly.
     *
     * @param event cluster changed event
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            final ClusterState state = event.state();
            RepositoriesMetadata oldMetadata = event.previousState().getMetadata().custom(RepositoriesMetadata.TYPE);
            RepositoriesMetadata newMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE);

            // Check if repositories got changed
            if ((oldMetadata == null && newMetadata == null) || (oldMetadata != null && oldMetadata.equalsIgnoreGenerations(newMetadata))) {
                for (Repository repo : repositories.values()) {
                    // Update State should only be invoked for repository which are already in cluster state. This
                    // check needs to be added as system repositories can be populated before cluster state has the
                    // repository metadata.
                    RepositoriesMetadata stateRepositoriesMetadata = state.metadata().custom(RepositoriesMetadata.TYPE);
                    if (stateRepositoriesMetadata != null && stateRepositoriesMetadata.repository(repo.getMetadata().name()) != null) {
                        repo.updateState(state);
                    }
                }
                return;
            }

            logger.trace("processing new index repositories for state version [{}]", event.state().version());

            Map<String, Repository> survivors = new HashMap<>();
            // First, remove repositories that are no longer there
            for (Map.Entry<String, Repository> entry : repositories.entrySet()) {
                if (newMetadata == null || newMetadata.repository(entry.getKey()) == null) {
                    logger.debug("unregistering repository [{}]", entry.getKey());
                    Repository repository = entry.getValue();
                    closeRepository(repository);
                } else {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }

            Map<String, Repository> builder = new HashMap<>();
            if (newMetadata != null) {
                // Now go through all repositories and update existing or create missing
                for (RepositoryMetadata repositoryMetadata : newMetadata.repositories()) {
                    Repository repository = survivors.get(repositoryMetadata.name());
                    if (repository != null) {
                        // Found previous version of this repository
                        RepositoryMetadata previousMetadata = repository.getMetadata();
                        if (previousMetadata.type().equals(repositoryMetadata.type()) == false
                            || previousMetadata.settings().equals(repositoryMetadata.settings()) == false) {
                            // Previous version is different from the version in settings
                            if (repository.isSystemRepository() && repository.isReloadable()) {
                                logger.debug(
                                    "updating repository [{}] in-place to use new metadata [{}]",
                                    repositoryMetadata.name(),
                                    repositoryMetadata
                                );
                                repository.validateMetadata(repositoryMetadata);
                                repository.reload(repositoryMetadata);
                            } else {
                                logger.debug("updating repository [{}]", repositoryMetadata.name());
                                closeRepository(repository);
                                repository = null;
                                try {
                                    repository = createRepository(repositoryMetadata, typesRegistry);
                                } catch (RepositoryException ex) {
                                    // TODO: this catch is bogus, it means the old repo is already closed,
                                    // but we have nothing to replace it
                                    logger.warn(
                                        () -> new ParameterizedMessage("failed to change repository [{}]", repositoryMetadata.name()),
                                        ex
                                    );
                                }
                            }
                        }
                    } else {
                        try {
                            // System repositories are already created and verified and hence during cluster state
                            // update we should avoid creating it again. Once the cluster state is update with the
                            // repository metadata the repository metadata update will land in the above if block.
                            if (repositories.containsKey(repositoryMetadata.name()) == false) {
                                repository = createRepository(repositoryMetadata, typesRegistry);
                            } else {
                                // Validate the repository metadata which was created during bootstrap is same as the
                                // one present in incoming cluster state.
                                repository = repositories.get(repositoryMetadata.name());
                                if (repositoryMetadata.equalsIgnoreGenerations(repository.getMetadata()) == false) {
                                    throw new RepositoryException(
                                        repositoryMetadata.name(),
                                        "repository was already " + "registered with different metadata during bootstrap than cluster state"
                                    );
                                }
                            }
                        } catch (RepositoryException ex) {
                            logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", repositoryMetadata.name()), ex);
                        }
                    }
                    if (repository != null) {
                        logger.debug("registering repository [{}]", repositoryMetadata.name());
                        builder.put(repositoryMetadata.name(), repository);
                    }
                }
            }
            for (Repository repo : builder.values()) {
                repo.updateState(state);
            }
            repositories = Collections.unmodifiableMap(builder);
        } catch (Exception ex) {
            assert false : new AssertionError(ex);
            logger.warn("failure updating cluster state ", ex);
        }
    }

    /**
     * Gets the {@link RepositoryData} for the given repository.
     *
     * @param repositoryName repository name
     * @param listener       listener to pass {@link RepositoryData} to
     */
    public void getRepositoryData(final String repositoryName, final ActionListener<RepositoryData> listener) {
        try {
            Repository repository = repository(repositoryName);
            assert repository != null; // should only be called once we've validated the repository exists
            repository.getRepositoryData(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns registered repository
     * <p>
     * This method is called only on the cluster-manager node
     *
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public Repository repository(String repositoryName) {
        Repository repository = repositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        repository = internalRepositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        throw new RepositoryMissingException(repositoryName);
    }

    public List<RepositoryStatsSnapshot> repositoriesStats() {
        List<RepositoryStatsSnapshot> activeRepoStats = getRepositoryStatsForActiveRepositories();
        return activeRepoStats;
    }

    public RepositoriesStats getRepositoriesStats() {
        return new RepositoriesStats(repositoriesStats());
    }

    private List<RepositoryStatsSnapshot> getRepositoryStatsForActiveRepositories() {
        return Stream.concat(repositories.values().stream(), internalRepositories.values().stream())
            .filter(r -> r instanceof MeteredBlobStoreRepository)
            .map(r -> (MeteredBlobStoreRepository) r)
            .map(MeteredBlobStoreRepository::statsSnapshot)
            .collect(Collectors.toList());
    }

    public List<RepositoryStatsSnapshot> clearRepositoriesStatsArchive(long maxVersionToClear) {
        return repositoriesStatsArchive.clear(maxVersionToClear);
    }

    public void registerInternalRepository(String name, String type) {
        RepositoryMetadata metadata = new RepositoryMetadata(name, type, Settings.EMPTY);
        Repository repository = internalRepositories.computeIfAbsent(name, (n) -> {
            logger.debug("put internal repository [{}][{}]", name, type);
            return createRepository(metadata, internalTypesRegistry);
        });
        if (type.equals(repository.getMetadata().type()) == false) {
            logger.warn(
                new ParameterizedMessage(
                    "internal repository [{}][{}] already registered. this prevented the registration of "
                        + "internal repository [{}][{}].",
                    name,
                    repository.getMetadata().type(),
                    name,
                    type
                )
            );
        } else if (repositories.containsKey(name)) {
            logger.warn(
                new ParameterizedMessage(
                    "non-internal repository [{}] already registered. this repository will block the "
                        + "usage of internal repository [{}][{}].",
                    name,
                    metadata.type(),
                    name
                )
            );
        }
    }

    public void unregisterInternalRepository(String name) {
        Repository repository = internalRepositories.remove(name);
        if (repository != null) {
            RepositoryMetadata metadata = repository.getMetadata();
            logger.debug(() -> new ParameterizedMessage("delete internal repository [{}][{}].", metadata.type(), name));
            closeRepository(repository);
        }
    }

    /** Closes the given repository. */
    public void closeRepository(Repository repository) {
        logger.debug("closing repository [{}][{}]", repository.getMetadata().type(), repository.getMetadata().name());
        repository.close();
    }

    /**
     * Creates repository holder. This method starts the non-internal repository
     */
    public Repository createRepository(RepositoryMetadata repositoryMetadata) {
        return this.createRepository(repositoryMetadata, typesRegistry);
    }

    /**
     * Creates repository holder. This method starts the repository
     */
    private Repository createRepository(RepositoryMetadata repositoryMetadata, Map<String, Repository.Factory> factories) {
        logger.debug("creating repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name());
        Repository.Factory factory = factories.get(repositoryMetadata.type());
        if (factory == null) {
            throw new RepositoryException(repositoryMetadata.name(), "repository type [" + repositoryMetadata.type() + "] does not exist");
        }
        Repository repository = null;
        try {
            repository = factory.create(repositoryMetadata, factories::get);
            repository.start();
            return repository;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(repository);
            logger.warn(
                new ParameterizedMessage("failed to create repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name()),
                e
            );
            throw new RepositoryException(repositoryMetadata.name(), "failed to create repository", e);
        }
    }

    public static void validate(final String identifier) {
        if (org.opensearch.core.common.Strings.hasLength(identifier) == false) {
            throw new RepositoryException(identifier, "cannot be empty");
        }
        if (identifier.contains("#")) {
            throw new RepositoryException(identifier, "must not contain '#'");
        }
        if (Strings.validFileName(identifier) == false) {
            throw new RepositoryException(identifier, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    public static void validateRepositoryMetadataSettings(
        ClusterService clusterService,
        final String repositoryName,
        final Settings repositoryMetadataSettings,
        Map<String, Repository> repositories,
        Settings settings,
        RepositoriesService repositoriesService
    ) {
        // We can add more validations here for repository settings in the future.
        Version minVersionInCluster = clusterService.state().getNodes().getMinNodeVersion();
        if (REMOTE_STORE_INDEX_SHALLOW_COPY.get(repositoryMetadataSettings) && !minVersionInCluster.onOrAfter(Version.V_2_9_0)) {
            throw new RepositoryException(
                repositoryName,
                "setting "
                    + REMOTE_STORE_INDEX_SHALLOW_COPY.getKey()
                    + " cannot be enabled as some of the nodes in cluster are on version older than "
                    + Version.V_2_9_0
                    + ". Minimum node version in cluster is: "
                    + minVersionInCluster
            );
        }
        if (SHALLOW_SNAPSHOT_V2.get(repositoryMetadataSettings)) {
            if (minVersionInCluster.onOrAfter(Version.V_2_17_0) == false) {
                throw new RepositoryException(
                    repositoryName,
                    "setting "
                        + SHALLOW_SNAPSHOT_V2.getKey()
                        + " cannot be enabled as some of the nodes in cluster are on version older than "
                        + Version.V_2_17_0
                        + ". Minimum node version in cluster is: "
                        + minVersionInCluster
                );
            }
            if (repositoryName.contains(SnapshotsService.SNAPSHOT_PINNED_TIMESTAMP_DELIMITER)) {
                throw new RepositoryException(
                    repositoryName,
                    "setting "
                        + SHALLOW_SNAPSHOT_V2.getKey()
                        + " cannot be enabled for repository with "
                        + SnapshotsService.SNAPSHOT_PINNED_TIMESTAMP_DELIMITER
                        + " in the name as this delimiter is used to create pinning entity"
                );
            }
            if (repositoryWithShallowV2Exists(repositories)) {
                throw new RepositoryException(
                    repositoryName,
                    "setting "
                        + SHALLOW_SNAPSHOT_V2.getKey()
                        + " cannot be enabled as this setting can be enabled only on one repository "
                        + " and one or more repositories in the cluster have the setting as enabled"
                );
            }
            try {
                if (pinnedTimestampExistsWithDifferentRepository(repositoryName, settings, repositoriesService)) {
                    throw new RepositoryException(
                        repositoryName,
                        "setting "
                            + SHALLOW_SNAPSHOT_V2.getKey()
                            + " cannot be enabled if there are existing snapshots created with shallow V2 "
                            + "setting using different repository."
                    );
                }
            } catch (IOException e) {
                throw new RepositoryException(repositoryName, "Exception while fetching pinned timestamp details");
            }
        }
        // Validation to not allow users to create system repository via put repository call.
        if (isSystemRepositorySettingPresent(repositoryMetadataSettings)) {
            throw new RepositoryException(
                repositoryName,
                "setting "
                    + SYSTEM_REPOSITORY_SETTING.getKey()
                    + " cannot provide system repository setting; this setting is managed by OpenSearch"
            );
        }
    }

    private static boolean repositoryWithShallowV2Exists(Map<String, Repository> repositories) {
        return repositories.values().stream().anyMatch(repo -> SHALLOW_SNAPSHOT_V2.get(repo.getMetadata().settings()));
    }

    private static boolean pinnedTimestampExistsWithDifferentRepository(
        String newRepoName,
        Settings settings,
        RepositoriesService repositoriesService
    ) throws IOException {
        Map<String, Set<Long>> pinningEntityTimestampMap = RemoteStorePinnedTimestampService.fetchPinnedTimestamps(
            settings,
            repositoriesService
        );
        for (String pinningEntity : pinningEntityTimestampMap.keySet()) {
            String repoNameWithPinnedTimestamps = pinningEntity.split(SnapshotsService.SNAPSHOT_PINNED_TIMESTAMP_DELIMITER)[0];
            if (repoNameWithPinnedTimestamps.equals(newRepoName) == false) {
                return true;
            }
        }
        return false;
    }

    private static void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        if (isRepositoryInUse(clusterState, repository)) {
            throw new IllegalStateException("trying to modify or unregister repository that is currently used");
        }
    }

    private static void ensureCryptoSettingsAreSame(RepositoryMetadata repositoryMetadata, PutRepositoryRequest request) {
        if (repositoryMetadata.cryptoMetadata() == null && request.cryptoSettings() == null) {
            return;
        }
        if (repositoryMetadata.cryptoMetadata() == null || request.cryptoSettings() == null) {
            throw new IllegalArgumentException("Crypto settings changes found in the repository update request. This is not allowed");
        }

        CryptoMetadata cryptoMetadata = repositoryMetadata.cryptoMetadata();
        CryptoSettings cryptoSettings = request.cryptoSettings();
        if (!cryptoMetadata.keyProviderName().equals(cryptoSettings.getKeyProviderName())
            || !cryptoMetadata.keyProviderType().equals(cryptoSettings.getKeyProviderType())
            || !cryptoMetadata.settings().toString().equals(cryptoSettings.getSettings().toString())) {
            throw new IllegalArgumentException("Changes in crypto settings found in the repository update request. This is not allowed");
        }
    }

    /**
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     */
    private static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        final SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        for (SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
            if (repository.equals(snapshot.snapshot().getRepository())) {
                return true;
            }
        }
        for (SnapshotDeletionsInProgress.Entry entry : clusterState.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        ).getEntries()) {
            if (entry.repository().equals(repository)) {
                return true;
            }
        }
        for (RepositoryCleanupInProgress.Entry entry : clusterState.custom(
            RepositoryCleanupInProgress.TYPE,
            RepositoryCleanupInProgress.EMPTY
        ).entries()) {
            if (entry.repository().equals(repository)) {
                return true;
            }
        }
        for (RestoreInProgress.Entry entry : clusterState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            if (repository.equals(entry.snapshot().getRepository())) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method will be used to update the repositories map.
     */
    public void updateRepositoriesMap(Map<String, Repository> repos) {
        if (repositories.isEmpty()) {
            repositories = repos;
        } else {
            throw new IllegalArgumentException("can't overwrite as repositories are already present");
        }
    }

    private static void ensureNotSystemRepository(RepositoryMetadata repositoryMetadata) {
        if (isSystemRepositorySettingPresent(repositoryMetadata.settings())) {
            throw new RepositoryException(repositoryMetadata.name(), "cannot delete a system repository");
        }
    }

    private static boolean isSystemRepositorySettingPresent(Settings repositoryMetadataSettings) {
        return SYSTEM_REPOSITORY_SETTING.get(repositoryMetadataSettings);
    }

    private static boolean isValueEqual(String key, String newValue, String currentValue) {
        if (newValue == null && currentValue == null) {
            return true;
        }
        if (newValue == null) {
            throw new IllegalArgumentException("[" + key + "] cannot be empty, " + "current value [" + currentValue + "]");
        }
        if (newValue.equals(currentValue) == false) {
            throw new IllegalArgumentException(
                "trying to modify an unmodifiable attribute "
                    + key
                    + " of system repository from "
                    + "current value ["
                    + currentValue
                    + "] to new value ["
                    + newValue
                    + "]"
            );
        }
        return true;
    }

    public void ensureValidSystemRepositoryUpdate(RepositoryMetadata newRepositoryMetadata, RepositoryMetadata currentRepositoryMetadata) {
        if (isSystemRepositorySettingPresent(currentRepositoryMetadata.settings())) {
            try {
                isValueEqual("type", newRepositoryMetadata.type(), currentRepositoryMetadata.type());

                Repository repository = repositories.get(currentRepositoryMetadata.name());
                Settings newRepositoryMetadataSettings = newRepositoryMetadata.settings();
                Settings currentRepositoryMetadataSettings = currentRepositoryMetadata.settings();

                List<String> restrictedSettings = repository.getRestrictedSystemRepositorySettings()
                    .stream()
                    .map(setting -> setting.getKey())
                    .collect(Collectors.toList());

                for (String restrictedSettingKey : restrictedSettings) {
                    isValueEqual(
                        restrictedSettingKey,
                        newRepositoryMetadataSettings.get(restrictedSettingKey),
                        currentRepositoryMetadataSettings.get(restrictedSettingKey)
                    );
                }
            } catch (IllegalArgumentException e) {
                throw new RepositoryException(currentRepositoryMetadata.name(), e.getMessage());
            }
        }
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        clusterService.removeApplier(this);
        final Collection<Repository> repos = new ArrayList<>();
        repos.addAll(internalRepositories.values());
        repos.addAll(repositories.values());
        IOUtils.close(repos);
    }
}
