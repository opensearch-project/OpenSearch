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

package org.opensearch.repositories.s3;

import com.amazonaws.util.json.Jackson;
import org.opensearch.SpecialPermission;
import org.opensearch.cluster.metadata.RepositoryMetadata;
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

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A plugin to add a repository type that writes to and from the AWS S3.
 */
public class S3RepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    static {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                // kick jackson to do some static caching of declared members info
                Jackson.jsonNodeOf("{}");
                // ClientConfiguration clinit has some classloader problems
                // TODO: fix that
                Class.forName("com.amazonaws.ClientConfiguration");
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    protected final S3Service service;
    private final Path configPath;

    public S3RepositoryPlugin(final Settings settings, final Path configPath) {
        this(settings, configPath, new S3Service(configPath));
    }

    S3RepositoryPlugin(final Settings settings, final Path configPath, final S3Service service) {
        this.service = Objects.requireNonNull(service, "S3 service must not be null");
        this.configPath = configPath;
        // eagerly load client settings so that secure settings are read
        final Map<String, S3ClientSettings> clientsSettings = S3ClientSettings.load(settings, configPath);
        this.service.refreshAndClearCache(clientsSettings);
    }

    // proxy method for testing
    protected S3Repository createRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry registry,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {
        return new S3Repository(metadata, registry, service, clusterService, recoverySettings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        final Environment env,
        final NamedXContentRegistry registry,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            S3Repository.TYPE,
            metadata -> createRepository(metadata, registry, clusterService, recoverySettings)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // named s3 client configuration settings
            S3ClientSettings.ACCESS_KEY_SETTING,
            S3ClientSettings.SECRET_KEY_SETTING,
            S3ClientSettings.SESSION_TOKEN_SETTING,
            S3ClientSettings.ENDPOINT_SETTING,
            S3ClientSettings.PROTOCOL_SETTING,
            S3ClientSettings.PROXY_HOST_SETTING,
            S3ClientSettings.PROXY_PORT_SETTING,
            S3ClientSettings.PROXY_USERNAME_SETTING,
            S3ClientSettings.PROXY_PASSWORD_SETTING,
            S3ClientSettings.READ_TIMEOUT_SETTING,
            S3ClientSettings.MAX_RETRIES_SETTING,
            S3ClientSettings.USE_THROTTLE_RETRIES_SETTING,
            S3ClientSettings.USE_PATH_STYLE_ACCESS,
            S3Repository.ACCESS_KEY_SETTING,
            S3Repository.SECRET_KEY_SETTING,
            S3ClientSettings.SIGNER_OVERRIDE,
            S3ClientSettings.REGION,
            S3ClientSettings.ROLE_ARN_SETTING,
            S3ClientSettings.IDENTITY_TOKEN_FILE_SETTING,
            S3ClientSettings.ROLE_SESSION_NAME_SETTING
        );
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, S3ClientSettings> clientsSettings = S3ClientSettings.load(settings, configPath);
        service.refreshAndClearCache(clientsSettings);
    }

    @Override
    public void close() throws IOException {
        service.close();
    }
}
