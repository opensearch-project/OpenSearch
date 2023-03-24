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

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.s3.S3ClientSettings.IrsaCredentials;
import org.opensearch.repositories.s3.async.TransferNIOGroup;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static com.amazonaws.SDKGlobalConfiguration.*;
import static java.util.Collections.emptyMap;

class S3AsyncService implements Closeable {
    private static final Logger logger = LogManager.getLogger(S3AsyncService.class);

    private static final String STS_ENDPOINT_OVERRIDE_SYSTEM_PROPERTY = "com.amazonaws.sdk.stsEndpointOverride";

    private volatile Map<S3ClientSettings, AmazonAsyncS3Reference> clientsCache = emptyMap();

    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile Map<String, S3ClientSettings> staticClientSettings;
    private volatile TransferNIOGroup transferNIOGroup;

    /**
     * Client settings derived from those in {@link #staticClientSettings} by combining them with settings
     * in the {@link RepositoryMetadata}.
     */
    private volatile Map<Settings, S3ClientSettings> derivedClientSettings = emptyMap();

    S3AsyncService(final Path configPath) {
        staticClientSettings = MapBuilder.<String, S3ClientSettings>newMapBuilder()
            .put("default", S3ClientSettings.getClientSettings(Settings.EMPTY, "default", configPath))
            .immutableMap();
    }

    /**
     * Refreshes the settings for the AmazonS3 clients and clears the cache of
     * existing clients. New clients will be build using these new settings. Old
     * clients are usable until released. On release they will be destroyed instead
     * of being returned to the cache.
     */
    public synchronized void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {
        // shutdown all unused clients
        // others will shutdown on their respective release
        releaseCachedClients();
        this.staticClientSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
        derivedClientSettings = emptyMap();
        assert this.staticClientSettings.containsKey("default") : "always at least have 'default'";
        // clients are built lazily by {@link client}
    }

    /**
     * Attempts to retrieve a client by its repository metadata and settings from the cache.
     * If the client does not exist it will be created.
     */
    public AmazonAsyncS3Reference client(RepositoryMetadata repositoryMetadata) {
        final S3ClientSettings clientSettings = settings(repositoryMetadata);
        {
            final AmazonAsyncS3Reference clientReference = clientsCache.get(clientSettings);
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }
        }
        synchronized (this) {
            final AmazonAsyncS3Reference existing = clientsCache.get(clientSettings);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }
            final AmazonAsyncS3Reference clientReference = new AmazonAsyncS3Reference(buildClient(clientSettings));
            clientReference.incRef();
            clientsCache = MapBuilder.newMapBuilder(clientsCache).put(clientSettings, clientReference).immutableMap();
            return clientReference;
        }
    }

    /**
     * Either fetches {@link S3ClientSettings} for a given {@link RepositoryMetadata} from cached settings or creates them
     * by overriding static client settings from {@link #staticClientSettings} with settings found in the repository metadata.
     * @param repositoryMetadata Repository Metadata
     * @return S3ClientSettings
     */
    S3ClientSettings settings(RepositoryMetadata repositoryMetadata) {
        final Settings settings = repositoryMetadata.settings();
        {
            final S3ClientSettings existing = derivedClientSettings.get(settings);
            if (existing != null) {
                return existing;
            }
        }
        final String clientName = S3Repository.CLIENT_NAME.get(settings);
        final S3ClientSettings staticSettings = staticClientSettings.get(clientName);
        if (staticSettings != null) {
            synchronized (this) {
                final S3ClientSettings existing = derivedClientSettings.get(settings);
                if (existing != null) {
                    return existing;
                }
                final S3ClientSettings newSettings = staticSettings.refine(settings);
                derivedClientSettings = MapBuilder.newMapBuilder(derivedClientSettings).put(settings, newSettings).immutableMap();
                return newSettings;
            }
        }
        throw new IllegalArgumentException(
            "Unknown s3 client name ["
                + clientName
                + "]. Existing client configs: "
                + Strings.collectionToDelimitedString(staticClientSettings.keySet(), ",")
        );
    }

    // proxy for testing
    synchronized AmazonAsyncS3WithCredentials buildClient(final S3ClientSettings clientSettings) {
        final S3AsyncClientBuilder builder = S3AsyncClient.builder();
        final AwsCredentialsProvider credentials = buildCredentials(logger, clientSettings);
        builder.credentialsProvider(credentials);
        if (transferNIOGroup == null) {
            transferNIOGroup = new TransferNIOGroup();
        }
        builder.httpClient(buildConfiguration(clientSettings, transferNIOGroup));

        String endpoint = Strings.hasLength(clientSettings.endpoint) ? clientSettings.endpoint : Constants.S3_HOSTNAME;
        if ((endpoint.startsWith("http://") || endpoint.startsWith("https://")) == false) {
            // Manually add the schema to the endpoint to work around https://github.com/aws/aws-sdk-java/issues/2274
            endpoint = clientSettings.protocol.toString() + "://" + endpoint;
        }
        final String region = Strings.hasLength(clientSettings.region) ? clientSettings.region : null;
        builder.region(region != null ? Region.of(region) : Region.US_EAST_1);
        logger.debug("using endpoint [{}] and region [{}]", endpoint, region);

        // If the endpoint configuration isn't set on the builder then the default behaviour is to try
        // and work out what region we are in and use an appropriate endpoint - see AwsClientBuilder#setRegion.
        // In contrast, directly-constructed clients use s3.amazonaws.com unless otherwise instructed. We currently
        // use a directly-constructed client, and need to keep the existing behaviour to avoid a breaking change,
        // so to move to using the builder we must set it explicitly to keep the existing behaviour.
        //
        // We do this because directly constructing the client is deprecated (was already deprecated in 1.1.223 too)
        // so this change removes that usage of a deprecated API.
        builder.endpointOverride(URI.create(endpoint));
        if (clientSettings.pathStyleAccess) {
            builder.forcePathStyle(true);
        }

        final S3AsyncClient client = SocketAccess.doPrivileged(builder::build);
        return AmazonAsyncS3WithCredentials.create(client, credentials);
    }

    // pkg private for tests
    static SdkAsyncHttpClient buildConfiguration(S3ClientSettings clientSettings, TransferNIOGroup transferNIOGroup) {
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        NettyNioAsyncHttpClient.Builder clientBuilder = NettyNioAsyncHttpClient.builder();

        if (clientSettings.proxySettings.getType() != ProxySettings.ProxyType.DIRECT) {
            ProxyConfiguration.Builder proxyConfiguration = ProxyConfiguration.builder();
            proxyConfiguration.scheme(clientSettings.proxySettings.getType().toProtocol().toString());
            proxyConfiguration.host(clientSettings.proxySettings.getHostName());
            proxyConfiguration.port(clientSettings.proxySettings.getPort());
            proxyConfiguration.username(clientSettings.proxySettings.getUsername());
            proxyConfiguration.password(clientSettings.proxySettings.getPassword());
            clientBuilder.proxyConfiguration(proxyConfiguration.build());
        }

        //TODO: add max retry and UseThrottleRetry. Replace values with settings and put these in default settings
        clientBuilder.connectionTimeout(Duration.ofSeconds(10));
        clientBuilder.connectionAcquisitionTimeout(Duration.ofMinutes(2));
        clientBuilder.maxPendingConnectionAcquires(10_000);
        clientBuilder.maxConcurrency(100);
        clientBuilder.eventLoopGroup(SdkEventLoopGroup.create(transferNIOGroup.getEventLoopGroup()));
        clientBuilder.tcpKeepAlive(true);

        return clientBuilder.build();
    }

    // pkg private for tests
    static AwsCredentialsProvider buildCredentials(Logger logger, S3ClientSettings clientSettings) {
        final S3BasicCredentials basicCredentials = clientSettings.credentials;
        final IrsaCredentials irsaCredentials = buildFromEnviroment(clientSettings.irsaCredentials);

        setDefaultAwsProfilePath();
        // If IAM Roles for Service Accounts (IRSA) credentials are configured, start with them first
        if (irsaCredentials != null) {
            logger.debug("Using IRSA credentials");
            AWSSecurityTokenService securityTokenService = null;
            final String region = Strings.hasLength(clientSettings.region) ? clientSettings.region : null;

            if (region != null || basicCredentials != null) {
                securityTokenService = SocketAccess.doPrivileged(() -> {
                    AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard();

                    // Use similar approach to override STS endpoint as SDKGlobalConfiguration.EC2_METADATA_SERVICE_OVERRIDE_SYSTEM_PROPERTY
                    final String stsEndpoint = System.getProperty(STS_ENDPOINT_OVERRIDE_SYSTEM_PROPERTY);
                    if (region != null && stsEndpoint != null) {
                        builder = builder.withEndpointConfiguration(new EndpointConfiguration(stsEndpoint, region));
                    } else {
                        builder = builder.withRegion(region);
                    }

                    if (basicCredentials != null) {
                        builder = builder.withCredentials(new AWSStaticCredentialsProvider(basicCredentials));
                    }

                    return builder.build();
                });
            }

            if (irsaCredentials.getIdentityTokenFile() == null) {
                final STSAssumeRoleSessionCredentialsProvider.Builder stsCredentialsProviderBuilder =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(irsaCredentials.getRoleArn(), irsaCredentials.getRoleSessionName())
                        .withStsClient(securityTokenService);

                final STSAssumeRoleSessionCredentialsProvider stsCredentialsProvider = SocketAccess.doPrivileged(
                    stsCredentialsProviderBuilder::build
                );

                return new PrivilegedSTSAssumeRoleSessionCredentialsProvider<>(securityTokenService,
                    new SessionsCredsWrapper<>(stsCredentialsProvider));
            } else {
                final STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder stsCredentialsProviderBuilder =
                    new STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder(
                        irsaCredentials.getRoleArn(),
                        irsaCredentials.getRoleSessionName(),
                        irsaCredentials.getIdentityTokenFile()
                    ).withStsClient(securityTokenService);

                final STSAssumeRoleWithWebIdentitySessionCredentialsProvider stsCredentialsProvider = SocketAccess.doPrivileged(
                    stsCredentialsProviderBuilder::build
                );

                return new PrivilegedSTSAssumeRoleSessionCredentialsProvider<>(securityTokenService,
                    new SessionsCredsWrapper<>(stsCredentialsProvider));
            }
        } else if (basicCredentials != null) {
            logger.debug("Using basic key/secret credentials");
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(basicCredentials.getAWSAccessKeyId(),
                basicCredentials.getAWSSecretKey()));
        } else {
            logger.debug("Using instance profile credentials");
            return InstanceProfileCredentialsProvider.builder().build();
        }
    }

    // Aws v2 sdk tries to load a default profile from home path which is restricted. Hence, setting these to random
    // valid paths.
    private static void setDefaultAwsProfilePath() {
        if (ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.getStringValue().isEmpty()) {
            System.setProperty(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property(),
                System.getProperty("opensearch.path.conf"));
        }
        if (ProfileFileSystemSetting.AWS_CONFIG_FILE.getStringValue().isEmpty()) {
            System.setProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property(),
                System.getProperty("opensearch.path.conf"));
        }
    }

    static class SessionsCredsWrapper <P extends AWSSessionCredentialsProvider & Closeable>
        implements AwsCredentialsProvider, Closeable {

        private final P sessionCredentialsProvider;

        public SessionsCredsWrapper(P sessionCredentialsProvider) {
            this.sessionCredentialsProvider = sessionCredentialsProvider;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            AWSSessionCredentials sessionCredentials = sessionCredentialsProvider.getCredentials();
            return AwsSessionCredentials.create(sessionCredentials.getAWSAccessKeyId(),
                sessionCredentials.getAWSSecretKey(), sessionCredentials.getSessionToken());
        }

        @Override
        public void close() throws IOException {
            sessionCredentialsProvider.close();
        }
    }

    private static IrsaCredentials buildFromEnviroment(IrsaCredentials defaults) {
        if (defaults == null) {
            return null;
        }

        String webIdentityTokenFile = defaults.getIdentityTokenFile();
        if (webIdentityTokenFile == null) {
            webIdentityTokenFile = System.getenv(AWS_WEB_IDENTITY_ENV_VAR);
        }

        String roleArn = defaults.getRoleArn();
        if (roleArn == null) {
            roleArn = System.getenv(AWS_ROLE_ARN_ENV_VAR);
        }

        String roleSessionName = defaults.getRoleSessionName();
        if (roleSessionName == null) {
            roleSessionName = System.getenv(AWS_ROLE_SESSION_NAME_ENV_VAR);
        }

        return new IrsaCredentials(webIdentityTokenFile, roleArn, roleSessionName);
    }

    private synchronized void releaseCachedClients() {
        // the clients will shutdown when they will not be used anymore
        for (final AmazonAsyncS3Reference clientReference : clientsCache.values()) {
            clientReference.decRef();
        }

        // clear previously cached clients, they will be build lazily
        clientsCache = emptyMap();
        derivedClientSettings = emptyMap();

        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    static class PrivilegedInstanceProfileCredentialsProvider implements AwsCredentialsProvider {
        private final AwsCredentialsProvider credentials;

        private PrivilegedInstanceProfileCredentialsProvider() {
            // InstanceProfileCredentialsProvider as last item of chain
            this.credentials = ContainerCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return SocketAccess.doPrivileged(credentials::resolveCredentials);
        }

    }

    static class PrivilegedSTSAssumeRoleSessionCredentialsProvider<P extends AwsCredentialsProvider & Closeable>
        implements
            AwsCredentialsProvider,
            Closeable {
        private final P credentials;
        private final AWSSecurityTokenService securityTokenService;

        private PrivilegedSTSAssumeRoleSessionCredentialsProvider(
            @Nullable final AWSSecurityTokenService securityTokenService,
            final P credentials
        ) {
            this.securityTokenService = securityTokenService;
            this.credentials = credentials;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return SocketAccess.doPrivileged(credentials::resolveCredentials);
        }

        @Override
        public void close() throws IOException {
            SocketAccess.doPrivilegedIOException(() -> {
                credentials.close();
                if (securityTokenService != null) {
                    securityTokenService.shutdown();
                }
                return null;
            });
        };
    }

    @Override
    public void close() {
        releaseCachedClients();
    }
}
