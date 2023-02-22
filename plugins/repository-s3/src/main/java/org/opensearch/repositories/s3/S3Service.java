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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.http.SystemPropertyTlsKeyManagersProvider;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.internal.SdkSSLContext;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.s3.S3ClientSettings.IrsaCredentials;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Map;

import static com.amazonaws.SDKGlobalConfiguration.AWS_ROLE_ARN_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.AWS_ROLE_SESSION_NAME_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.AWS_WEB_IDENTITY_ENV_VAR;
import static java.util.Collections.emptyMap;

class S3Service implements Closeable {
    private static final Logger logger = LogManager.getLogger(S3Service.class);

    private static final String STS_ENDPOINT_OVERRIDE_SYSTEM_PROPERTY = "com.amazonaws.sdk.stsEndpointOverride";

    private volatile Map<S3ClientSettings, AmazonS3Reference> clientsCache = emptyMap();

    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile Map<String, S3ClientSettings> staticClientSettings;

    /**
     * Client settings derived from those in {@link #staticClientSettings} by combining them with settings
     * in the {@link RepositoryMetadata}.
     */
    private volatile Map<Settings, S3ClientSettings> derivedClientSettings = emptyMap();

    S3Service(final Path configPath) {
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
    public AmazonS3Reference client(RepositoryMetadata repositoryMetadata) {
        final S3ClientSettings clientSettings = settings(repositoryMetadata);
        {
            final AmazonS3Reference clientReference = clientsCache.get(clientSettings);
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }
        }
        synchronized (this) {
            final AmazonS3Reference existing = clientsCache.get(clientSettings);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }
            final AmazonS3Reference clientReference = new AmazonS3Reference(buildClient(clientSettings));
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
    AmazonS3WithCredentials buildClient(final S3ClientSettings clientSettings) {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

        final AWSCredentialsProvider credentials = buildCredentials(logger, clientSettings);
        builder.withCredentials(credentials);
        builder.withClientConfiguration(buildConfiguration(clientSettings));

        String endpoint = Strings.hasLength(clientSettings.endpoint) ? clientSettings.endpoint : Constants.S3_HOSTNAME;
        if ((endpoint.startsWith("http://") || endpoint.startsWith("https://")) == false) {
            // Manually add the schema to the endpoint to work around https://github.com/aws/aws-sdk-java/issues/2274
            // TODO: Remove this once fixed in the AWS SDK
            endpoint = clientSettings.protocol.toString() + "://" + endpoint;
        }
        final String region = Strings.hasLength(clientSettings.region) ? clientSettings.region : null;
        logger.debug("using endpoint [{}] and region [{}]", endpoint, region);

        // If the endpoint configuration isn't set on the builder then the default behaviour is to try
        // and work out what region we are in and use an appropriate endpoint - see AwsClientBuilder#setRegion.
        // In contrast, directly-constructed clients use s3.amazonaws.com unless otherwise instructed. We currently
        // use a directly-constructed client, and need to keep the existing behaviour to avoid a breaking change,
        // so to move to using the builder we must set it explicitly to keep the existing behaviour.
        //
        // We do this because directly constructing the client is deprecated (was already deprecated in 1.1.223 too)
        // so this change removes that usage of a deprecated API.
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        if (clientSettings.pathStyleAccess) {
            builder.enablePathStyleAccess();
        }
        if (clientSettings.disableChunkedEncoding) {
            builder.disableChunkedEncoding();
        }
        final AmazonS3 client = SocketAccess.doPrivileged(builder::build);
        return AmazonS3WithCredentials.create(client, credentials);
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(S3ClientSettings clientSettings) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(clientSettings.protocol);

        if (clientSettings.proxySettings != ProxySettings.NO_PROXY_SETTINGS) {
            if (clientSettings.proxySettings.getType() == ProxySettings.ProxyType.SOCKS) {
                SocketAccess.doPrivilegedVoid(() -> {
                    if (clientSettings.proxySettings.isAuthenticated()) {
                        Authenticator.setDefault(new Authenticator() {
                            @Override
                            protected PasswordAuthentication getPasswordAuthentication() {
                                return new PasswordAuthentication(
                                    clientSettings.proxySettings.getUsername(),
                                    clientSettings.proxySettings.getPassword().toCharArray()
                                );
                            }
                        });
                    }
                    clientConfiguration.getApacheHttpClientConfig()
                        .setSslSocketFactory(createSocksSslConnectionSocketFactory(clientSettings.proxySettings.getAddress()));
                });
            } else {
                if (clientSettings.proxySettings.getType() != ProxySettings.ProxyType.DIRECT) {
                    clientConfiguration.setProxyProtocol(clientSettings.proxySettings.getType().toProtocol());
                }
                clientConfiguration.setProxyHost(clientSettings.proxySettings.getHostName());
                clientConfiguration.setProxyPort(clientSettings.proxySettings.getPort());
                clientConfiguration.setProxyUsername(clientSettings.proxySettings.getUsername());
                clientConfiguration.setProxyPassword(clientSettings.proxySettings.getPassword());
            }
        }

        if (Strings.hasLength(clientSettings.signerOverride)) {
            clientConfiguration.setSignerOverride(clientSettings.signerOverride);
        }

        clientConfiguration.setMaxErrorRetry(clientSettings.maxRetries);
        clientConfiguration.setUseThrottleRetries(clientSettings.throttleRetries);
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);

        return clientConfiguration;
    }

    private static SSLConnectionSocketFactory createSocksSslConnectionSocketFactory(final InetSocketAddress address) {
        // This part was taken from AWS settings
        final SSLContext sslCtx = SdkSSLContext.getPreferredSSLContext(
            new SystemPropertyTlsKeyManagersProvider().getKeyManagers(),
            new SecureRandom()
        );
        return new SdkTLSSocketFactory(sslCtx, new DefaultHostnameVerifier()) {
            @Override
            public Socket createSocket(final HttpContext ctx) throws IOException {
                return new Socket(new Proxy(Proxy.Type.SOCKS, address));
            }
        };
    }

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(Logger logger, S3ClientSettings clientSettings) {
        final S3BasicCredentials basicCredentials = clientSettings.credentials;
        final IrsaCredentials irsaCredentials = buildFromEnviroment(clientSettings.irsaCredentials);

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

                return new PrivilegedSTSAssumeRoleSessionCredentialsProvider<>(securityTokenService, stsCredentialsProvider);
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

                return new PrivilegedSTSAssumeRoleSessionCredentialsProvider<>(securityTokenService, stsCredentialsProvider);
            }
        } else if (basicCredentials != null) {
            logger.debug("Using basic key/secret credentials");
            return new AWSStaticCredentialsProvider(basicCredentials);
        } else {
            logger.debug("Using instance profile credentials");
            return new PrivilegedInstanceProfileCredentialsProvider();
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
        for (final AmazonS3Reference clientReference : clientsCache.values()) {
            clientReference.decRef();
        }

        // clear previously cached clients, they will be build lazily
        clientsCache = emptyMap();
        derivedClientSettings = emptyMap();

        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    static class PrivilegedInstanceProfileCredentialsProvider implements AWSCredentialsProvider {
        private final AWSCredentialsProvider credentials;

        private PrivilegedInstanceProfileCredentialsProvider() {
            // InstanceProfileCredentialsProvider as last item of chain
            this.credentials = new EC2ContainerCredentialsProviderWrapper();
        }

        @Override
        public AWSCredentials getCredentials() {
            return SocketAccess.doPrivileged(credentials::getCredentials);
        }

        @Override
        public void refresh() {
            SocketAccess.doPrivilegedVoid(credentials::refresh);
        }
    }

    static class PrivilegedSTSAssumeRoleSessionCredentialsProvider<P extends AWSSessionCredentialsProvider & Closeable>
        implements
            AWSCredentialsProvider,
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
        public AWSCredentials getCredentials() {
            return SocketAccess.doPrivileged(credentials::getCredentials);
        }

        @Override
        public void refresh() {
            SocketAccess.doPrivilegedVoid(credentials::refresh);
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
