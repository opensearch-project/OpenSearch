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

package org.opensearch.plugin.discovery.azure.classic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cloud.azure.classic.management.AzureComputeService;
import org.opensearch.cloud.azure.classic.management.AzureComputeServiceImpl;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.SeedHostsProvider;
import org.opensearch.discovery.azure.classic.AzureSeedHostsProvider;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class AzureDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    public static final String AZURE = "azure";
    protected final Settings settings;
    private static final Logger logger = LogManager.getLogger(AzureDiscoveryPlugin.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(AzureDiscoveryPlugin.class);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        deprecationLogger.deprecate("azure_discovery_plugin", "azure classic discovery plugin is deprecated.");
        logger.trace("starting azure classic discovery plugin...");
    }

    // overrideable for tests
    protected AzureComputeService createComputeService() {
        return new AzureComputeServiceImpl(settings);
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        return Collections.singletonMap(
            AZURE,
            () -> createSeedHostsProvider(settings, createComputeService(), transportService, networkService)
        );
    }

    // Used for testing
    protected AzureSeedHostsProvider createSeedHostsProvider(
        final Settings settings,
        final AzureComputeService azureComputeService,
        final TransportService transportService,
        final NetworkService networkService
    ) {
        return new AzureSeedHostsProvider(settings, azureComputeService, transportService, networkService);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureComputeService.Discovery.REFRESH_SETTING,
            AzureComputeService.Management.KEYSTORE_PASSWORD_SETTING,
            AzureComputeService.Management.KEYSTORE_PATH_SETTING,
            AzureComputeService.Management.KEYSTORE_TYPE_SETTING,
            AzureComputeService.Management.SUBSCRIPTION_ID_SETTING,
            AzureComputeService.Management.SERVICE_NAME_SETTING,
            AzureComputeService.Discovery.HOST_TYPE_SETTING,
            AzureComputeService.Discovery.DEPLOYMENT_NAME_SETTING,
            AzureComputeService.Discovery.DEPLOYMENT_SLOT_SETTING,
            AzureComputeService.Discovery.ENDPOINT_NAME_SETTING
        );
    }
}
