/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

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
import org.opensearch.secure_sm.AccessController;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.emptyMap;

class S3AsyncService implements Closeable {
    private static final Logger logger = LogManager.getLogger(S3AsyncService.class);

    private static final String STS_ENDPOINT_OVERRIDE_SYSTEM_PROPERTY = "aws.stsEndpointOverride";

    // We will need to support the cache with both type of clients. Since S3ClientSettings doesn't contain Http Client.
    // Also adding the Http Client type in S3ClientSettings is not good option since it is used by Async and Sync clients.
    // We can segregate the types of cache here itself
    private volatile Map<String, Map<S3ClientSettings, AmazonAsyncS3Reference>> s3HttpClientTypesClientsCache = emptyMap();

    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile Map<String, S3ClientSettings> staticClientSettings;

    /**
     * Client settings derived from those in {@link #staticClientSettings} by combining them with settings
     * in the {@link RepositoryMetadata}.
     */
    private volatile Map<Settings, S3ClientSettings> derivedClientSettings = emptyMap();

    /**
     * Optional scheduled executor service to use for the client
     */
    private final @Nullable ScheduledExecutorService clientExecutorService;

    S3AsyncService(final Path configPath, @Nullable ScheduledExecutorService clientExecutorService) {

        staticClientSettings = MapBuilder.<String, S3ClientSettings>newMapBuilder()
            .put(
                buildClientName("default", S3Repository.CRT_ASYNC_HTTP_CLIENT_TYPE),
                S3ClientSettings.getClientSettings(Settings.EMPTY, "default", configPath)
            )
            .put(
                buildClientName("default", S3Repository.NETTY_ASYNC_HTTP_CLIENT_TYPE),
                S3ClientSettings.getClientSettings(Settings.EMPTY, "default", configPath)
            )
            .immutableMap();
        this.clientExecutorService = clientExecutorService;
    }

    private String buildClientName(final String clientValue, final String asyncClientType) {
        return clientValue + "-" + asyncClientType;
    }

    S3AsyncService(final Path configPath) {
        this(configPath, null);
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
        MapBuilder<String, S3ClientSettings> defaultBuilder = MapBuilder.newMapBuilder();
        for (Map.Entry<String, S3ClientSettings> entrySet : clientsSettings.entrySet()) {
            defaultBuilder.put(
                buildClientName(entrySet.getKey(), S3Repository.CRT_ASYNC_HTTP_CLIENT_TYPE),
                clientsSettings.get(entrySet.getKey())
            );
            defaultBuilder.put(
                buildClientName(entrySet.getKey(), S3Repository.NETTY_ASYNC_HTTP_CLIENT_TYPE),
                clientsSettings.get(entrySet.getKey())
            );
        }

        staticClientSettings = defaultBuilder.immutableMap();
        derivedClientSettings = emptyMap();
        assert this.staticClientSettings.containsKey(buildClientName("default", S3Repository.NETTY_ASYNC_HTTP_CLIENT_TYPE))
            : "Static Client Settings should contain default Netty client";
        assert this.staticClientSettings.containsKey(buildClientName("default", S3Repository.CRT_ASYNC_HTTP_CLIENT_TYPE))
            : "Static Client Settings should contain default CRT client";
        // clients are built lazily by {@link client}
    }

    /**
     * Attempts to retrieve a client by its repository metadata and settings from the cache.
     * If the client does not exist it will be created.
     */
    public AmazonAsyncS3Reference client(
        RepositoryMetadata repositoryMetadata,
        AsyncExecutorContainer urgentExecutorBuilder,
        AsyncExecutorContainer priorityExecutorBuilder,
        AsyncExecutorContainer normalExecutorBuilder
    ) {
        String asyncHttpClientType = S3Repository.S3_ASYNC_HTTP_CLIENT_TYPE.get(repositoryMetadata.settings());

        final S3ClientSettings clientSettings = settings(repositoryMetadata);
        AmazonAsyncS3Reference clientReference = getCachedClientForHttpTypeAndClientSettings(asyncHttpClientType, clientSettings);
        if (clientReference != null) {
            return clientReference;
        }

        synchronized (this) {
            AmazonAsyncS3Reference existingClient = getCachedClientForHttpTypeAndClientSettings(asyncHttpClientType, clientSettings);
            if (existingClient != null) {
                return existingClient;
            }

            // If the client reference is not found in cache. Let's create it.
            final AmazonAsyncS3Reference newClientReference = new AmazonAsyncS3Reference(
                buildClient(clientSettings, urgentExecutorBuilder, priorityExecutorBuilder, normalExecutorBuilder, asyncHttpClientType)
            );
            newClientReference.incRef();

            // Get or create new client cache map for the HTTP client type
            Map<S3ClientSettings, AmazonAsyncS3Reference> clientsCacheForType = s3HttpClientTypesClientsCache.getOrDefault(
                asyncHttpClientType,
                emptyMap()
            );

            // Update both cache levels atomically
            s3HttpClientTypesClientsCache = MapBuilder.newMapBuilder(s3HttpClientTypesClientsCache)
                .put(
                    asyncHttpClientType,
                    MapBuilder.newMapBuilder(clientsCacheForType).put(clientSettings, newClientReference).immutableMap()
                )
                .immutableMap();
            return newClientReference;
        }
    }

    private AmazonAsyncS3Reference getCachedClientForHttpTypeAndClientSettings(
        final String asyncHttpClientType,
        final S3ClientSettings clientSettings
    ) {
        final Map<S3ClientSettings, AmazonAsyncS3Reference> clientsCacheMap = s3HttpClientTypesClientsCache.get(asyncHttpClientType);
        if (clientsCacheMap != null && !clientsCacheMap.isEmpty()) {
            final AmazonAsyncS3Reference clientReference = clientsCacheMap.get(clientSettings);
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }
        }
        return null;
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
        final String clientName = buildClientName(
            S3Repository.CLIENT_NAME.get(settings),
            S3Repository.S3_ASYNC_HTTP_CLIENT_TYPE.get(repositoryMetadata.settings())
        );
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
        AsyncExecutorContainer urgentExecutorBuilder,
        AsyncExecutorContainer priorityExecutorBuilder,
        AsyncExecutorContainer normalExecutorBuilder,
        String asyncHttpClientType
    ) {
        setDefaultAwsProfilePath();
        final S3AsyncClientBuilder builder = S3AsyncClient.builder();
        builder.overrideConfiguration(buildOverrideConfiguration(clientSettings, clientExecutorService));
        final AwsCredentialsProvider credentials = buildCredentials(logger, clientSettings);
        builder.credentialsProvider(credentials);

        // Only apply endpointOverride when the user explicitly configures "endpoint".
        // If "endpoint" is absent, DO NOT override; allow the AWS SDK to resolve endpoints dynamically.
        // This is required for ARN buckets (access points / outposts / MRAP), and is also correct for normal buckets.
        resolveEndpointOverride(clientSettings).ifPresent(builder::endpointOverride);
        builder.region(Region.of(clientSettings.region));
        if (clientSettings.pathStyleAccess) {
            builder.forcePathStyle(true);
        }

        builder.httpClient(buildHttpClient(clientSettings, urgentExecutorBuilder.getAsyncTransferEventLoopGroup(), asyncHttpClientType));
        builder.asyncConfiguration(
            ClientAsyncConfiguration.builder()
                .advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                    urgentExecutorBuilder.getFutureCompletionExecutor()
                )
                .build()
        );
        final S3AsyncClient urgentClient = AccessController.doPrivileged(builder::build);

        builder.httpClient(buildHttpClient(clientSettings, priorityExecutorBuilder.getAsyncTransferEventLoopGroup(), asyncHttpClientType));
        builder.asyncConfiguration(
            ClientAsyncConfiguration.builder()
                .advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                    priorityExecutorBuilder.getFutureCompletionExecutor()
                )
                .build()
        );
        final S3AsyncClient priorityClient = AccessController.doPrivileged(builder::build);

        builder.httpClient(buildHttpClient(clientSettings, normalExecutorBuilder.getAsyncTransferEventLoopGroup(), asyncHttpClientType));
        builder.asyncConfiguration(
            ClientAsyncConfiguration.builder()
                .advancedOption(
                    SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                    normalExecutorBuilder.getFutureCompletionExecutor()
                )
                .build()
        );
        builder.responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED);
        if (clientSettings.legacyMd5ChecksumCalculation) {
            builder.addPlugin(LegacyMd5Plugin.create());
        }
        final S3AsyncClient client = AccessController.doPrivileged(builder::build);
        return AmazonAsyncS3WithCredentials.create(client, priorityClient, urgentClient, credentials);
    }

    /**
     * Returns an endpoint override ONLY when the user explicitly provided one.
     * Otherwise returns Optional.empty().
     *
     * Package-private to allow unit testing.
     */
    Optional<URI> resolveEndpointOverride(final S3ClientSettings clientSettings) {
        if (Strings.hasLength(clientSettings.endpoint) == false) {
            return Optional.empty();
        }

        String endpoint = clientSettings.endpoint;
        if ((endpoint.startsWith("http://") || endpoint.startsWith("https://")) == false) {
            // Manually add the schema to the endpoint to work around https://github.com/aws/aws-sdk-java/issues/2274
            // TODO: Remove this once fixed in the AWS SDK
            endpoint = clientSettings.protocol.toString() + "://" + endpoint;
        }

        // Use URI.create for simplicity; if your codebase prefers checked handling, swap to new URI(endpoint).
        return Optional.of(URI.create(endpoint));
    }

    static SdkAsyncHttpClient buildHttpClient(
        S3ClientSettings clientSettings,
        AsyncTransferEventLoopGroup asyncTransferEventLoopGroup,
        final String asyncHttpClientType
    ) {
        logger.debug("S3 Http client type [{}]", asyncHttpClientType);
        if (S3Repository.NETTY_ASYNC_HTTP_CLIENT_TYPE.equals(asyncHttpClientType)) {
            return buildAsyncNettyHttpClient(clientSettings, asyncTransferEventLoopGroup);
        }
        return buildAsyncCrtHttpClient(clientSettings);
    }

    static SdkAsyncHttpClient buildAsyncNettyHttpClient(
        final S3ClientSettings clientSettings,
        final AsyncTransferEventLoopGroup asyncTransferEventLoopGroup
    ) {
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        NettyNioAsyncHttpClient.Builder clientBuilder = NettyNioAsyncHttpClient.builder();

        if (clientSettings.proxySettings.getType() != ProxySettings.ProxyType.DIRECT) {
            software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder proxyConfiguration =
                software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder();
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

    static SdkAsyncHttpClient buildAsyncCrtHttpClient(final S3ClientSettings clientSettings) {
        AwsCrtAsyncHttpClient.Builder crtClientBuilder = AwsCrtAsyncHttpClient.builder();

        if (clientSettings.proxySettings.getType() != ProxySettings.ProxyType.DIRECT) {
            ProxyConfiguration.Builder crtProxyConfiguration = ProxyConfiguration.builder();

            crtProxyConfiguration.scheme(clientSettings.proxySettings.getType().toProtocol().toString());
            crtProxyConfiguration.host(clientSettings.proxySettings.getHostName());
            crtProxyConfiguration.port(clientSettings.proxySettings.getPort());
            crtProxyConfiguration.username(clientSettings.proxySettings.getUsername());
            crtProxyConfiguration.password(clientSettings.proxySettings.getPassword());

            crtClientBuilder.proxyConfiguration(crtProxyConfiguration.build());
        }

        crtClientBuilder.connectionTimeout(Duration.ofMillis(clientSettings.connectionTimeoutMillis));
        crtClientBuilder.maxConcurrency(clientSettings.maxConnections);
        return crtClientBuilder.build();
    }

    static ClientOverrideConfiguration buildOverrideConfiguration(
        final S3ClientSettings clientSettings,
        ScheduledExecutorService clientExecutorService
    ) {
        RetryPolicy retryPolicy = AccessController.doPrivileged(
            () -> RetryPolicy.builder()
                .numRetries(clientSettings.maxRetries)
                .throttlingBackoffStrategy(
                    clientSettings.throttleRetries ? BackoffStrategy.defaultThrottlingStrategy(RetryMode.STANDARD) : BackoffStrategy.none()
                )
                .build()
        );
        ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();
        if (clientExecutorService != null) {
            builder = builder.scheduledExecutorService(clientExecutorService);
        }

        return builder.retryPolicy(retryPolicy).apiCallAttemptTimeout(Duration.ofMillis(clientSettings.requestTimeoutMillis)).build();
    }

    // pkg private for tests
    static AwsCredentialsProvider buildCredentials(Logger logger, S3ClientSettings clientSettings) {
        final AwsCredentials basicCredentials = clientSettings.credentials;
        final IrsaCredentials irsaCredentials = buildFromEnvironment(clientSettings.irsaCredentials);

        // If IAM Roles for Service Accounts (IRSA) credentials are configured, start with them first
        if (irsaCredentials != null) {
            logger.debug("Using IRSA credentials");

            final Region region = Region.of(clientSettings.region);
            StsClient stsClient = AccessController.doPrivileged(() -> {
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

                final StsAssumeRoleCredentialsProvider stsCredentialsProvider = AccessController.doPrivileged(
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

                final StsWebIdentityTokenFileCredentialsProvider stsCredentialsProvider = AccessController.doPrivileged(
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
        S3Service.setDefaultAwsProfilePath();
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

    public synchronized void releaseCachedClients() {
        // There will be 2 types of caches CRT and Netty
        for (Map<S3ClientSettings, AmazonAsyncS3Reference> clientTypeCaches : s3HttpClientTypesClientsCache.values()) {
            // the clients will shutdown when they will not be used anymore
            for (final AmazonAsyncS3Reference clientReference : clientTypeCaches.values()) {
                clientReference.decRef();
            }
        }

        // clear previously cached clients, they will be build lazily
        s3HttpClientTypesClientsCache = emptyMap();
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
            return AccessController.doPrivileged(credentials::resolveCredentials);
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
            try {
                AccessController.doPrivilegedChecked(() -> {
                    credentials.close();
                    if (stsClient != null) {
                        stsClient.close();
                    }
                });
            } catch (Exception e) {
                throw (IOException) e;
            }
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AccessController.doPrivileged(credentials::resolveCredentials);
        }
    }

    @Override
    public void close() {
        releaseCachedClients();
    }

    @Nullable
    ScheduledExecutorService getClientExecutorService() {
        return clientExecutorService;
    }
}
