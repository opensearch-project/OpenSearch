/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.kms;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;

import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Service class which exposes APIs for communication with AWS KMS.
 */
public class KmsService implements Closeable {

    private static final Logger logger = LogManager.getLogger(KmsService.class);
    private final CredentialProviderFactory credentialProviderFactory;

    static final Setting<String> ENC_CTX_SETTING = Setting.simpleString("kms.encryption_context", Setting.Property.NodeScope);

    static final Setting<String> KEY_ARN_SETTING = Setting.simpleString("kms.key_arn", Setting.Property.NodeScope);

    private volatile Map<KmsClientSettings, AmazonKmsClientReference> clientsCache = emptyMap();

    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile KmsClientSettings staticClientSettings;

    /**
     * Client settings derived from those in {@link #staticClientSettings} by combining them with crypto settings
     */
    private volatile Map<Settings, KmsClientSettings> derivedClientSettings;

    public KmsService() {
        credentialProviderFactory = new CredentialProviderFactory();
    }

    private KmsClient buildClient(KmsClientSettings clientSettings) {
        SocketAccess.doPrivilegedVoid(KmsService::setDefaultAwsProfilePath);
        final AwsCredentialsProvider awsCredentialsProvider = buildCredentials(clientSettings);
        final ClientOverrideConfiguration overrideConfiguration = buildOverrideConfiguration();
        final ProxyConfiguration proxyConfiguration = SocketAccess.doPrivileged(() -> buildProxyConfiguration(clientSettings));
        return buildClient(
            awsCredentialsProvider,
            proxyConfiguration,
            overrideConfiguration,
            clientSettings.endpoint,
            clientSettings.region,
            clientSettings.readTimeoutMillis
        );
    }

    // proxy for testing
    protected KmsClient buildClient(
        AwsCredentialsProvider awsCredentialsProvider,
        ProxyConfiguration proxyConfiguration,
        ClientOverrideConfiguration overrideConfiguration,
        String endpoint,
        String region,
        long readTimeoutMillis
    ) {
        ApacheHttpClient.Builder clientBuilder = ApacheHttpClient.builder()
            .proxyConfiguration(proxyConfiguration)
            .socketTimeout(Duration.ofMillis(readTimeoutMillis));

        KmsClientBuilder builder = KmsClient.builder()
            .region(Region.of(region))
            .overrideConfiguration(overrideConfiguration)
            .httpClientBuilder(clientBuilder)
            .credentialsProvider(awsCredentialsProvider);

        if (Strings.hasText(endpoint)) {
            logger.debug("using explicit kms endpoint [{}]", endpoint);
            builder.endpointOverride(URI.create(endpoint));
        }

        if (Strings.hasText(region)) {
            logger.debug("using explicit kms region [{}]", region);
            builder.region(Region.of(region));
        }

        return SocketAccess.doPrivileged(builder::build);
    }

    ProxyConfiguration buildProxyConfiguration(KmsClientSettings clientSettings) {
        if (Strings.hasText(clientSettings.proxyHost)) {
            try {
                return ProxyConfiguration.builder()
                    .endpoint(new URI("https", null, clientSettings.proxyHost, clientSettings.proxyPort, null, null, null))
                    .username(clientSettings.proxyUsername)
                    .password(clientSettings.proxyPassword)
                    .build();
            } catch (URISyntaxException e) {
                throw SdkException.create("Invalid proxy URL", e);
            }
        } else {
            return ProxyConfiguration.builder().build();
        }
    }

    ClientOverrideConfiguration buildOverrideConfiguration() {
        return ClientOverrideConfiguration.builder().retryPolicy(buildRetryPolicy()).build();
    }

    // pkg private for tests
    RetryPolicy buildRetryPolicy() {
        // Increase the number of retries in case of 5xx API responses.
        // Note that AWS SDK v2 introduced a concept of TokenBucketRetryCondition, which effectively limits retries for
        // APIs that have been failing continuously. It allocates tokens (default is 500), which means that once 500
        // retries fail for any API on a bucket, new retries will only be allowed once some retries are rejected.
        // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/conditions/TokenBucketRetryCondition.html
        RetryPolicy.Builder retryPolicy = RetryPolicy.builder().numRetries(10);
        return retryPolicy.build();
    }

    AwsCredentialsProvider buildCredentials(KmsClientSettings clientSettings) {
        final AwsCredentials credentials = clientSettings.credentials;
        return credentialProviderFactory.createAwsCredentialsProvider(() -> credentials);
    }

    public AmazonKmsClientReference client(CryptoMetadata cryptoMetadata) {
        final KmsClientSettings clientSettings = settings(cryptoMetadata);
        {
            final AmazonKmsClientReference clientReference = clientsCache.get(clientSettings);
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }
        }
        synchronized (this) {
            final AmazonKmsClientReference existing = clientsCache.get(clientSettings);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }
            final AmazonKmsClientReference clientReference = new AmazonKmsClientReference(
                SocketAccess.doPrivileged(() -> buildClient(clientSettings))
            );
            clientReference.incRef();
            clientsCache = MapBuilder.newMapBuilder(clientsCache).put(clientSettings, clientReference).immutableMap();
            return clientReference;
        }
    }

    /**
     * Either fetches {@link KmsClientSettings} for a given {@link CryptoMetadata} from cached settings or creates them
     * by overriding static client settings from {@link #staticClientSettings} with settings found in the crypto metadata.
     * @param cryptoMetadata Crypto Metadata
     * @return KmsClientSettings
     */
    KmsClientSettings settings(CryptoMetadata cryptoMetadata) {
        final Settings settings = cryptoMetadata.settings();
        {
            final KmsClientSettings existing = derivedClientSettings.get(settings);
            if (existing != null) {
                return existing;
            }
        }
        synchronized (this) {
            final KmsClientSettings existing = derivedClientSettings.get(settings);
            if (existing != null) {
                return existing;
            }
            final KmsClientSettings newSettings = staticClientSettings.getMetadataSettings(settings);
            derivedClientSettings = MapBuilder.newMapBuilder(derivedClientSettings).put(settings, newSettings).immutableMap();
            return newSettings;
        }
    }

    /**
     * Refreshes the settings for the AmazonKMS client. The new client will be build
     * using these new settings. The old client is usable until released. On release it
     * will be destroyed instead of being returned to the cache.
     */
    public void refreshAndClearCache(KmsClientSettings clientSettings) {
        // shutdown all unused clients
        // others will shutdown on their respective release
        releaseCachedClients();
        this.staticClientSettings = clientSettings;
        derivedClientSettings = emptyMap();
    }

    private synchronized void releaseCachedClients() {
        // the clients will shutdown when they will not be used anymore
        for (final AmazonKmsClientReference clientReference : clientsCache.values()) {
            clientReference.decRef();
        }

        // clear previously cached clients, they will be build lazily
        clientsCache = emptyMap();
        derivedClientSettings = emptyMap();
    }

    @Override
    public void close() {
        releaseCachedClients();
    }

    // By default, AWS v2 SDK loads a default profile from $USER_HOME, which is restricted. Use the OpenSearch configuration path instead.
    @SuppressForbidden(reason = "Prevent AWS SDK v2 from using ~/.aws/config and ~/.aws/credentials.")
    static void setDefaultAwsProfilePath() {
        if (ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.getStringValue().isEmpty()) {
            logger.info("setting aws.sharedCredentialsFile={}", System.getProperty("opensearch.path.conf"));
            System.setProperty(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property(), System.getProperty("opensearch.path.conf"));
        }
        if (ProfileFileSystemSetting.AWS_CONFIG_FILE.getStringValue().isEmpty()) {
            logger.info("setting aws.sharedCredentialsFile={}", System.getProperty("opensearch.path.conf"));
            System.setProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property(), System.getProperty("opensearch.path.conf"));
        }
    }

    public MasterKeyProvider createMasterKeyProvider(CryptoMetadata cryptoMetadata) {
        String keyArn = KEY_ARN_SETTING.get(cryptoMetadata.settings());
        if (!Strings.hasText(keyArn)) {
            throw new IllegalArgumentException("Missing key_arn setting");
        }

        String kmsEncCtx = ENC_CTX_SETTING.get(cryptoMetadata.settings());
        Map<String, String> encCtx;
        if (Strings.hasText(kmsEncCtx)) {
            try {
                encCtx = Arrays.stream(kmsEncCtx.split(","))
                    .map(s -> s.split("="))
                    .collect(Collectors.toMap(e -> e[0].trim(), e -> e[1].trim()));
            } catch (Exception ex) {
                throw new IllegalArgumentException(ENC_CTX_SETTING.getKey() + " Format should be: Name1=Value1, Name2=Value2");
            }
        } else {
            encCtx = new HashMap<>();
        }

        // Verify client creation is successful to early detect any failure.
        try (AmazonKmsClientReference clientReference = client(cryptoMetadata)) {
            clientReference.get();
        }

        return new KmsMasterKeyProvider(encCtx, keyArn, () -> client(cryptoMetadata));
    }
}
