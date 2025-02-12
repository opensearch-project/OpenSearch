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

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sets up classes for Snapshot/Restore.
 *
 * @opensearch.internal
 */
public final class RepositoriesModule {

    private final RepositoriesService repositoriesService;

    public RepositoriesModule(
        Environment env,
        List<RepositoryPlugin> repoPlugins,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        NamedXContentRegistry namedXContentRegistry,
        RecoverySettings recoverySettings
    ) {
        Map<String, Repository.Factory> factories = new HashMap<>();
        factories.put(
            FsRepository.TYPE,
            metadata -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
        );

        factories.put(
            ReloadableFsRepository.TYPE,
            metadata -> new ReloadableFsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
        );

        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getRepositories(
                env,
                namedXContentRegistry,
                clusterService,
                recoverySettings
            );
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Repository type [" + entry.getKey() + "] is already registered");
                }
            }
        }

        Map<String, Repository.Factory> internalFactories = new HashMap<>();
        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getInternalRepositories(
                env,
                namedXContentRegistry,
                clusterService,
                recoverySettings
            );
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (internalFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Internal repository type [" + entry.getKey() + "] is already registered");
                }
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException(
                        "Internal repository type [" + entry.getKey() + "] is already registered as a " + "non-internal repository"
                    );
                }
            }
        }

        Settings settings = env.settings();
        Map<String, Repository.Factory> repositoryTypes = Collections.unmodifiableMap(factories);
        Map<String, Repository.Factory> internalRepositoryTypes = Collections.unmodifiableMap(internalFactories);
        repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            repositoryTypes,
            internalRepositoryTypes,
            threadPool
        );
    }

    public RepositoriesService getRepositoryService() {
        return repositoriesService;
    }
}
