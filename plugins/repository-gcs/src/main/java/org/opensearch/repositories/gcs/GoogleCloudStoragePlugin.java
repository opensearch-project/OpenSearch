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

package org.opensearch.repositories.gcs;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GoogleCloudStoragePlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    // package-private for tests
    final GoogleCloudStorageService storageService;

    public GoogleCloudStoragePlugin(final Settings settings) {
        this.storageService = createStorageService();
        // eagerly load client settings so that secure settings are readable (not closed)
        reload(settings);
    }

    // overridable for tests
    protected GoogleCloudStorageService createStorageService() {
        return new GoogleCloudStorageService();
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            GoogleCloudStorageRepository.TYPE,
            metadata -> new GoogleCloudStorageRepository(
                metadata,
                namedXContentRegistry,
                this.storageService,
                clusterService,
                recoverySettings
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING,
            GoogleCloudStorageClientSettings.ENDPOINT_SETTING,
            GoogleCloudStorageClientSettings.PROJECT_ID_SETTING,
            GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING,
            GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING,
            GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING,
            GoogleCloudStorageClientSettings.TOKEN_URI_SETTING,
            GoogleCloudStorageClientSettings.PROXY_TYPE_SETTING,
            GoogleCloudStorageClientSettings.PROXY_HOST_SETTING,
            GoogleCloudStorageClientSettings.PROXY_PORT_SETTING,
            GoogleCloudStorageClientSettings.PROXY_USERNAME_SETTING,
            GoogleCloudStorageClientSettings.PROXY_PASSWORD_SETTING,
            GoogleCloudStorageClientSettings.TRUSTSTORE_PATH_SETTING,
            GoogleCloudStorageClientSettings.TRUSTSTORE_PASSWORD_SETTING,
            GoogleCloudStorageClientSettings.TRUSTSTORE_TYPE_SETTING
        );
    }

    @Override
    public void reload(Settings settings) {
        // Secure settings should be readable inside this method. Duplicate client
        // settings in a format (`GoogleCloudStorageClientSettings`) that does not
        // require for the `SecureSettings` to be open. Pass that around (the
        // `GoogleCloudStorageClientSettings` instance) instead of the `Settings`
        // instance.
        final Map<String, GoogleCloudStorageClientSettings> clientsSettings = GoogleCloudStorageClientSettings.load(settings);
        this.storageService.refreshAndClearCache(clientsSettings);
    }
}
