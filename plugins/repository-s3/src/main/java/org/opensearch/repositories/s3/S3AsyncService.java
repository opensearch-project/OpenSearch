/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.repositories.s3.S3ClientSettings.IrsaCredentials;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static java.util.Collections.emptyMap;

class S3AsyncService implements Closeable {
    private static final Logger logger = LogManager.getLogger(S3AsyncService.class);

    private static final String STS_ENDPOINT_OVERRIDE_SYSTEM_PROPERTY = "aws.stsEndpointOverride";

    private static final String DEFAULT_S3_ENDPOINT = "s3.amazonaws.com";

    private volatile Map<S3ClientSettings, AmazonAsyncS3Reference> clientsCache = emptyMap();

    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile Map<String, S3ClientSettings> staticClientSettings;

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
    public AmazonAsyncS3Reference client(
        RepositoryMetadata repositoryMetadata,
        AsyncExecutorContainer priorityExecutorBuilder,
        AsyncExecutorContainer normalExecutorBuilder
    ) {
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
            final AmazonAsyncS3Reference clientReference = new AmazonAsyncS3Reference(
                buildClient(clientSettings, priorityExecutorBuilder, normalExecutorBuilder)
            );
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
    synchronized AmazonAsyncS3WithCredentials buildClient(
        final S3ClientSettings clientSettings,
        AsyncExecutorContainer priorityExecutorBuilder,
        AsyncExecutorContainer normalExecutorBuilder
    ) {
        setDefaultAwsProfilePath();
        final S3AsyncClientBuilder builder = S3AsyncClient.builder();
        builder.overrideConfiguration(buildOverrideConfiguration(clientSettings));
        final AwsCredentialsProvider credentials = buildCredentials(logger, clientSettings);
        builder.credentialsProvider(credentials);

        String endpoint = Strings.hasLength(clientSettings.endpoint) ? clientSettings.endpoint : DEFAULT_S3_ENDPOINT;
        if ((endpoint.startsWith("http://") || endpoint.startsWith("https://")) == false) {
            // Manually add the schema to the endpoint to work around https://github.com/aws/aws-sdk-java/issues/2274
            endpoint = clientSettings.protocol.toString() + "://" + endpoint;
        }
        logger.debug("using endpoint [{}] and region [{}]", endpoint, clientSettings.region);

        // If the endpoint configuration isn't set on the builder then the default behaviour is to try
        // and work out what region we are in and use an appropriate endpoint - see AwsClientBuilder#setRegion.
        // In contrast, directly-constructed clients use s3.amazonaws.com unless otherwise instructed. We currently
        // use a directly-constructed client, and need to keep the existing behaviour to avoid a breaking change,
        // so to move to using the builder we must set it explicitly to keep the existing behaviour.
        //
        // We do this because directly constructing the client is deprecated (was already deprecated in 1.1.223 too)
        // so this change removes that usage of a deprecated API.
        builder.endpointOverride(URI.create(endpoint));
        builder.region(Region.of(clientSettings.region));
        if (clientSettings.pathStyleAccess) {
            builder.forcePathStyle(true);
        }

        builder.httpClient(buildHttpClient(clientSettings, priorityExecutorBuilder.getAsyncTransferEventLoopGroup()));
        builder.asyncConfiguration(
            ClientAsyncConfiguration.builder()
                .advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                    priorityExecutorBuilder.getFutureCompletionExecutor()
                )
                .build()
        );
        final S3AsyncClient priorityClient = SocketAccess.doPrivileged(builder::build);

        builder.httpClient(buildHttpClient(clientSettings, normalExecutorBuilder.getAsyncTransferEventLoopGroup()));
        builder.asyncConfiguration(
            ClientAsyncConfiguration.builder()
                .advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                    normalExecutorBuilder.getFutureCompletionExecutor()
                )
                .build()
        );
        final S3AsyncClient client = SocketAccess.doPrivileged(builder::build);

        return AmazonAsyncS3WithCredentials.create(client, priorityClient, credentials);
    }

    static ClientOverrideConfiguration buildOverrideConfiguration(final S3ClientSettings clientSettings) {
        return ClientOverrideConfiguration.builder()
            .retryPolicy(
                RetryPolicy.builder()
                    .numRetries(clientSettings.maxRetries)
                    .throttlingBackoffStrategy(
                        clientSettings.throttleRetries ? BackoffStrategy.defaultThrottlingStrategy() : BackoffStrategy.none()
                    )
                    .build()
            )
            .apiCallAttemptTimeout(Duration.ofMillis(clientSettings.requestTimeoutMillis))
            .build();
    }

    // pkg private for tests
    static SdkAsyncHttpClient buildHttpClient(S3ClientSettings clientSettings, AsyncTransferEventLoopGroup asyncTransferEventLoopGroup) {
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

        // TODO: add max retry and UseThrottleRetry. Replace values with settings and put these in default settings
        clientBuilder.connectionTimeout(Duration.ofMillis(clientSettings.connectionTimeoutMillis));
        clientBuilder.connectionAcquisitionTimeout(Duration.ofMillis(clientSettings.connectionAcquisitionTimeoutMillis));
        clientBuilder.maxPendingConnectionAcquires(10_000);
        clientBuilder.maxConcurrency(clientSettings.maxConnections);
        clientBuilder.eventLoopGroup(SdkEventLoopGroup.create(asyncTransferEventLoopGroup.getEventLoopGroup()));
        clientBuilder.tcpKeepAlive(true);

        return clientBuilder.build();
    }

    // pkg private for tests
    static AwsCredentialsProvider buildCredentials(Logger logger, S3ClientSettings clientSettings) {
        final AwsCredentials basicCredentials = clientSettings.credentials;
        final IrsaCredentials irsaCredentials = buildFromEnvironment(clientSettings.irsaCredentials);

        // If IAM Roles for Service Accounts (IRSA) credentials are configured, start with them first
        if (irsaCredentials != null) {
            logger.debug("Using IRSA credentials");

            final Region region = Region.of(clientSettings.region);
            StsClient stsClient = SocketAccess.doPrivileged(() -> {
                StsClientBuilder builder = StsClient.builder().region(region);

                final String stsEndpoint = System.getProperty(STS_ENDPOINT_OVERRIDE_SYSTEM_PROPERTY);
                if (stsEndpoint != null) {
                    builder = builder.endpointOverride(URI.create(stsEndpoint));
                }

                if (basicCredentials != null) {
                    builder = builder.credentialsProvider(StaticCredentialsProvider.create(basicCredentials));
                } else {
                    builder = builder.credentialsProvider(DefaultCredentialsProvider.create());
                }

                return builder.build();
            });

            if (irsaCredentials.getIdentityTokenFile() == null) {
                final StsAssumeRoleCredentialsProvider.Builder stsCredentialsProviderBuilder = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                        AssumeRoleRequest.builder()
                            .roleArn(irsaCredentials.getRoleArn())
                            .roleSessionName(irsaCredentials.getRoleSessionName())
                            .build()
                    );

                final StsAssumeRoleCredentialsProvider stsCredentialsProvider = SocketAccess.doPrivileged(
                    stsCredentialsProviderBuilder::build
                );

                return new PrivilegedSTSAssumeRoleSessionCredentialsProvider<>(stsClient, stsCredentialsProvider);
            } else {
                final StsWebIdentityTokenFileCredentialsProvider.Builder stsCredentialsProviderBuilder =
                    StsWebIdentityTokenFileCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .roleArn(irsaCredentials.getRoleArn())
                        .roleSessionName(irsaCredentials.getRoleSessionName())
                        .webIdentityTokenFile(Path.of(irsaCredentials.getIdentityTokenFile()));

                final StsWebIdentityTokenFileCredentialsProvider stsCredentialsProvider = SocketAccess.doPrivileged(
                    stsCredentialsProviderBuilder::build
                );

                return new PrivilegedSTSAssumeRoleSessionCredentialsProvider<>(stsClient, stsCredentialsProvider);
            }
        } else if (basicCredentials != null) {
            logger.debug("Using basic key/secret credentials");
            return StaticCredentialsProvider.create(basicCredentials);
        } else {
            logger.debug("Using instance profile credentials");
            return new PrivilegedInstanceProfileCredentialsProvider();
        }
    }

    // Aws v2 sdk tries to load a default profile from home path which is restricted. Hence, setting these to random
    // valid paths.
    @SuppressForbidden(reason = "Need to provide this override to v2 SDK so that path does not default to home path")
    private static void setDefaultAwsProfilePath() {
        if (ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.getStringValue().isEmpty()) {
            System.setProperty(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property(), System.getProperty("opensearch.path.conf"));
        }
        if (ProfileFileSystemSetting.AWS_CONFIG_FILE.getStringValue().isEmpty()) {
            System.setProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property(), System.getProperty("opensearch.path.conf"));
        }
    }

    private static IrsaCredentials buildFromEnvironment(IrsaCredentials defaults) {
        if (defaults == null) {
            return null;
        }

        String webIdentityTokenFile = defaults.getIdentityTokenFile();
        if (webIdentityTokenFile == null) {
            webIdentityTokenFile = System.getenv(SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE.environmentVariable());
        }

        String roleArn = defaults.getRoleArn();
        if (roleArn == null) {
            roleArn = System.getenv(SdkSystemSetting.AWS_ROLE_ARN.environmentVariable());
        }

        String roleSessionName = defaults.getRoleSessionName();
        if (roleSessionName == null) {
            roleSessionName = System.getenv(SdkSystemSetting.AWS_ROLE_SESSION_NAME.environmentVariable());
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
    }

    static class PrivilegedInstanceProfileCredentialsProvider implements AwsCredentialsProvider {
        private final AwsCredentialsProvider credentials;

        private PrivilegedInstanceProfileCredentialsProvider() {
            this.credentials = initializeProvider();
        }

        private AwsCredentialsProvider initializeProvider() {
            if (SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI.getStringValue().isPresent()
                || SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.getStringValue().isPresent()) {

                return ContainerCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
            }
            // InstanceProfileCredentialsProvider as last item of chain
            return InstanceProfileCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return SocketAccess.doPrivileged(credentials::resolveCredentials);
        }
    }

    static class PrivilegedSTSAssumeRoleSessionCredentialsProvider<P extends AwsCredentialsProvider & AutoCloseable>
        implements
            AwsCredentialsProvider,
            Closeable {
        private final P credentials;
        private final StsClient stsClient;

        private PrivilegedSTSAssumeRoleSessionCredentialsProvider(@Nullable final StsClient stsClient, final P credentials) {
            this.stsClient = stsClient;
            this.credentials = credentials;
        }

        @Override
        public void close() throws IOException {
            SocketAccess.doPrivilegedIOException(() -> {
                credentials.close();
                if (stsClient != null) {
                    stsClient.close();
                }
                return null;
            });
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return SocketAccess.doPrivileged(credentials::resolveCredentials);
        }
    }

    @Override
    public void close() {
        releaseCachedClients();
    }
}
