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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.repositories.s3.S3ClientSettings.ACCESS_KEY_SETTING;
import static org.opensearch.repositories.s3.S3ClientSettings.SECRET_KEY_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@SuppressForbidden(reason = "test requires to set a System property to allow insecure settings when running in IDE")
public class RepositoryCredentialsTests extends OpenSearchSingleNodeTestCase implements ConfigPathSupport {

    static {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            // required for client settings overwriting when running in IDE
            System.setProperty("opensearch.allow_insecure_settings", "true");
            return null;
        });
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(ProxyS3RepositoryPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Settings nodeSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING.getConcreteSettingForNamespace("default").getKey(), "secure_default_key");
        secureSettings.setString(SECRET_KEY_SETTING.getConcreteSettingForNamespace("default").getKey(), "secure_default_secret");
        secureSettings.setString(ACCESS_KEY_SETTING.getConcreteSettingForNamespace("other").getKey(), "secure_other_key");
        secureSettings.setString(SECRET_KEY_SETTING.getConcreteSettingForNamespace("other").getKey(), "secure_other_secret");

        return Settings.builder().setSecureSettings(secureSettings).put(super.nodeSettings()).build();
    }

    public void testRepositoryCredentialsOverrideSecureCredentials() {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        final String repositoryName = "repo-creds-override";
        final Settings.Builder repositorySettings = Settings.builder()
            // repository settings for credentials override node secure settings
            .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key")
            .put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret");

        final String clientName = randomFrom("default", "other", null);
        if (clientName != null) {
            repositorySettings.put(S3Repository.CLIENT_NAME.getKey(), clientName);
        }
        createRepository(repositoryName, repositorySettings.build());

        final RepositoriesService repositories = getInstanceFromNode(RepositoriesService.class);
        assertThat(repositories.repository(repositoryName), notNullValue());
        assertThat(repositories.repository(repositoryName), instanceOf(S3Repository.class));

        final S3Repository repository = (S3Repository) repositories.repository(repositoryName);
        try (final AmazonS3Reference clientReference = repository.createBlobStore().clientReference()) {
            S3Client client = clientReference.get();
            assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

            final AwsCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.resolveCredentials();
            assertThat(credentials.accessKeyId(), is("insecure_aws_key"));
            assertThat(credentials.secretAccessKey(), is("insecure_aws_secret"));
        }

        assertWarnings(
            "[secret_key] setting was deprecated in OpenSearch and will be removed in a future release!"
                + " See the breaking changes documentation for the next major version.",
            "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                + " the opensearch keystore for secure settings.",
            "[access_key] setting was deprecated in OpenSearch and will be removed in a future release!"
                + " See the breaking changes documentation for the next major version."
        );
    }

    public void testReinitSecureCredentials() {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        final String clientName = randomFrom("default", "other");

        final Settings.Builder repositorySettings = Settings.builder();
        final boolean hasInsecureSettings = randomBoolean();
        if (hasInsecureSettings) {
            // repository settings for credentials override node secure settings
            repositorySettings.put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key");
            repositorySettings.put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret");
        } else {
            repositorySettings.put(S3Repository.CLIENT_NAME.getKey(), clientName);
        }

        final String repositoryName = "repo-reinit-creds";
        createRepository(repositoryName, repositorySettings.build());

        final RepositoriesService repositories = getInstanceFromNode(RepositoriesService.class);
        assertThat(repositories.repository(repositoryName), notNullValue());
        assertThat(repositories.repository(repositoryName), instanceOf(S3Repository.class));

        final S3Repository repository = (S3Repository) repositories.repository(repositoryName);
        try (AmazonS3Reference clientReference = ((S3BlobStore) repository.blobStore()).clientReference()) {
            final S3Client client = clientReference.get();
            assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

            final AwsCredentials credentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.resolveCredentials();
            if (hasInsecureSettings) {
                assertThat(credentials.accessKeyId(), is("insecure_aws_key"));
                assertThat(credentials.secretAccessKey(), is("insecure_aws_secret"));
            } else if ("other".equals(clientName)) {
                assertThat(credentials.accessKeyId(), is("secure_other_key"));
                assertThat(credentials.secretAccessKey(), is("secure_other_secret"));
            } else {
                assertThat(credentials.accessKeyId(), is("secure_default_key"));
                assertThat(credentials.secretAccessKey(), is("secure_default_secret"));
            }

            // new settings
            final MockSecureSettings newSecureSettings = new MockSecureSettings();
            newSecureSettings.setString("s3.client." + clientName + ".access_key", "new_secret_aws_key");
            newSecureSettings.setString("s3.client." + clientName + ".secret_key", "new_secret_aws_secret");
            final Settings newSettings = Settings.builder().setSecureSettings(newSecureSettings).build();
            // reload S3 plugin settings
            final PluginsService plugins = getInstanceFromNode(PluginsService.class);
            final ProxyS3RepositoryPlugin plugin = plugins.filterPlugins(ProxyS3RepositoryPlugin.class).get(0);
            plugin.reload(newSettings);

            // check the not-yet-closed client reference still has the same credentials
            if (hasInsecureSettings) {
                assertThat(credentials.accessKeyId(), is("insecure_aws_key"));
                assertThat(credentials.secretAccessKey(), is("insecure_aws_secret"));
            } else if ("other".equals(clientName)) {
                assertThat(credentials.accessKeyId(), is("secure_other_key"));
                assertThat(credentials.secretAccessKey(), is("secure_other_secret"));
            } else {
                assertThat(credentials.accessKeyId(), is("secure_default_key"));
                assertThat(credentials.secretAccessKey(), is("secure_default_secret"));
            }
        }

        // check credentials have been updated
        try (AmazonS3Reference clientReference = ((S3BlobStore) repository.blobStore()).clientReference()) {
            final S3Client client = clientReference.get();
            assertThat(client, instanceOf(ProxyS3RepositoryPlugin.ClientAndCredentials.class));

            final AwsCredentials newCredentials = ((ProxyS3RepositoryPlugin.ClientAndCredentials) client).credentials.resolveCredentials();
            if (hasInsecureSettings) {
                assertThat(newCredentials.accessKeyId(), is("insecure_aws_key"));
                assertThat(newCredentials.secretAccessKey(), is("insecure_aws_secret"));
            } else {
                assertThat(newCredentials.accessKeyId(), is("new_secret_aws_key"));
                assertThat(newCredentials.secretAccessKey(), is("new_secret_aws_secret"));
            }
        }

        if (hasInsecureSettings) {
            assertWarnings(
                "[secret_key] setting was deprecated in OpenSearch and will be removed in a future release!"
                    + " See the breaking changes documentation for the next major version.",
                "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                    + " the opensearch keystore for secure settings.",
                "[access_key] setting was deprecated in OpenSearch and will be removed in a future release!"
                    + " See the breaking changes documentation for the next major version."
            );
        }
    }

    public void testInsecureRepositoryCredentials() throws Exception {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        final String repositoryName = "repo-insecure-creds";
        createRepository(
            repositoryName,
            Settings.builder()
                .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "insecure_aws_key")
                .put(S3Repository.SECRET_KEY_SETTING.getKey(), "insecure_aws_secret")
                .build()
        );

        final RestRequest fakeRestRequest = new FakeRestRequest();
        fakeRestRequest.params().put("repository", repositoryName);
        final RestGetRepositoriesAction action = new RestGetRepositoriesAction(getInstanceFromNode(SettingsFilter.class));

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> error = new AtomicReference<>();
        action.handleRequest(fakeRestRequest, new AbstractRestChannel(fakeRestRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    String responseAsString = response.content().utf8ToString();
                    assertThat(responseAsString, containsString(repositoryName));
                    assertThat(responseAsString, not(containsString("insecure_")));
                } catch (final AssertionError ex) {
                    error.set(ex);
                }
                latch.countDown();
            }
        }, getInstanceFromNode(NodeClient.class));

        latch.await();
        if (error.get() != null) {
            throw error.get();
        }

        assertWarnings(
            "Using s3 access/secret key from repository settings. Instead store these in named clients and"
                + " the opensearch keystore for secure settings."
        );
    }

    private void createRepository(final String name, final Settings repositorySettings) {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(name)
                .setType(S3Repository.TYPE)
                .setVerify(false)
                .setSettings(repositorySettings)
        );
    }

    /**
     * A S3 repository plugin that keeps track of the credentials used to build an AmazonS3 client
     */
    public static final class ProxyS3RepositoryPlugin extends S3RepositoryPlugin {

        public ProxyS3RepositoryPlugin(Settings settings, Path configPath) {
            super(settings, configPath, new ProxyS3Service(configPath), new S3AsyncService(configPath));
        }

        @Override
        protected S3Repository createRepository(
            RepositoryMetadata metadata,
            NamedXContentRegistry registry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return new S3Repository(metadata, registry, service, clusterService, recoverySettings, null, null, null, null, false) {
                @Override
                protected void assertSnapshotOrGenericThread() {
                    // eliminate thread name check as we create repo manually on test/main threads
                }
            };
        }

        public static final class ClientAndCredentials extends DelegatingS3Client {
            final AwsCredentialsProvider credentials;

            ClientAndCredentials(S3Client delegate, AwsCredentialsProvider credentials) {
                super(delegate);
                this.credentials = credentials;
            }
        }

        public static final class ProxyS3Service extends S3Service {

            private static final Logger logger = LogManager.getLogger(ProxyS3Service.class);

            ProxyS3Service(final Path configPath) {
                super(configPath);
            }

            @Override
            AmazonS3WithCredentials buildClient(final S3ClientSettings clientSettings) {
                final AmazonS3WithCredentials client = SocketAccess.doPrivileged(() -> super.buildClient(clientSettings));
                final AwsCredentialsProvider credentials = buildCredentials(logger, clientSettings);
                return AmazonS3WithCredentials.create(new ClientAndCredentials(client.client(), credentials), credentials);
            }

        }
    }
}
