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

package org.opensearch.repositories.azure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin, ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(AzureRepositoryPlugin.class);

    public static final String REPOSITORY_THREAD_POOL_NAME = "repository_azure";

    // protected for testing
    final AzureStorageService azureStoreService;

    /** Native store provider loaded via ExtensiblePlugin — null if no native plugin is present. */
    private NativeRemoteObjectStoreProvider nativeStoreProvider;

    public AzureRepositoryPlugin(Settings settings) {
        // eagerly load client settings so that secure settings are read
        this.azureStoreService = createAzureStoreService(settings);
    }

    // non-static, package private for testing
    AzureStorageService createAzureStoreService(final Settings settings) {
        return new AzureStorageService(settings);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<NativeRemoteObjectStoreProvider> providers = loader.loadExtensions(NativeRemoteObjectStoreProvider.class);
        int matchCount = 0;
        for (NativeRemoteObjectStoreProvider provider : providers) {
            if (AzureRepository.TYPE.equals(provider.repositoryType())) {
                matchCount++;
                if (this.nativeStoreProvider == null) {
                    this.nativeStoreProvider = provider;
                    logger.info(
                        "Loaded native object store provider [{}] for repository type [{}]",
                        provider.getClass().getName(),
                        AzureRepository.TYPE
                    );
                }
            }
        }
        if (matchCount > 1) {
            logger.warn(
                "Multiple native object store providers found for repository type [{}], using [{}]",
                AzureRepository.TYPE,
                this.nativeStoreProvider.getClass().getName()
            );
        }
        if (this.nativeStoreProvider == null) {
            logger.info("No native object store provider found for repository type [{}]", AzureRepository.TYPE);
        }
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            AzureRepository.TYPE,
            (metadata) -> new AzureRepository(
                metadata,
                namedXContentRegistry,
                azureStoreService,
                clusterService,
                recoverySettings,
                nativeStoreProvider
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureStorageSettings.ACCOUNT_SETTING,
            AzureStorageSettings.KEY_SETTING,
            AzureStorageSettings.SAS_TOKEN_SETTING,
            AzureStorageSettings.TOKEN_CREDENTIAL_TYPE_SETTING,
            AzureStorageSettings.ENDPOINT_SUFFIX_SETTING,
            AzureStorageSettings.TIMEOUT_SETTING,
            AzureStorageSettings.MAX_RETRIES_SETTING,
            AzureStorageSettings.CONNECT_TIMEOUT_SETTING,
            AzureStorageSettings.WRITE_TIMEOUT_SETTING,
            AzureStorageSettings.READ_TIMEOUT_SETTING,
            AzureStorageSettings.RESPONSE_TIMEOUT_SETTING,
            AzureStorageSettings.PROXY_TYPE_SETTING,
            AzureStorageSettings.PROXY_HOST_SETTING,
            AzureStorageSettings.PROXY_PORT_SETTING,
            AzureStorageSettings.PROXY_USERNAME_SETTING,
            AzureStorageSettings.PROXY_PASSWORD_SETTING
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(executorBuilder());
    }

    public static ExecutorBuilder<?> executorBuilder() {
        return new ScalingExecutorBuilder(REPOSITORY_THREAD_POOL_NAME, 0, 32, TimeValue.timeValueSeconds(30L));
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        if (clientsSettings.isEmpty()) {
            throw new SettingsException("If you want to use an azure repository, you need to define a client configuration.");
        }
        azureStoreService.refreshAndClearCache(clientsSettings);
    }
}
