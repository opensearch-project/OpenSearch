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

package org.opensearch.discovery.ec2;

import java.net.URI;
import java.net.URISyntaxException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.util.LazyInitializable;
import org.opensearch.core.common.Strings;
import org.opensearch.common.SuppressForbidden;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.core.retry.RetryPolicy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;

class AwsEc2ServiceImpl implements AwsEc2Service {
    private static final Logger logger = LogManager.getLogger(AwsEc2ServiceImpl.class);

    private final AtomicReference<LazyInitializable<AmazonEc2ClientReference, OpenSearchException>> lazyClientReference =
        new AtomicReference<>();

    private Ec2Client buildClient(Ec2ClientSettings clientSettings) {
        SocketAccess.doPrivilegedVoid(AwsEc2ServiceImpl::setDefaultAwsProfilePath);
        final AwsCredentialsProvider awsCredentialsProvider = buildCredentials(logger, clientSettings);
        final ClientOverrideConfiguration overrideConfiguration = buildOverrideConfiguration(logger, clientSettings);
        final ProxyConfiguration proxyConfiguration = SocketAccess.doPrivileged(() -> buildProxyConfiguration(logger, clientSettings));
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
    protected Ec2Client buildClient(
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

        Ec2ClientBuilder builder = Ec2Client.builder()
            .overrideConfiguration(overrideConfiguration)
            .httpClientBuilder(clientBuilder)
            .credentialsProvider(awsCredentialsProvider);

        if (Strings.hasText(endpoint)) {
            logger.debug("using explicit ec2 endpoint [{}]", endpoint);
            builder.endpointOverride(URI.create(endpoint));
        }

        if (Strings.hasText(region)) {
            logger.debug("using explicit ec2 region [{}]", region);
            builder.region(Region.of(region));
        }

        return SocketAccess.doPrivileged(builder::build);
    }

    static ProxyConfiguration buildProxyConfiguration(Logger logger, Ec2ClientSettings clientSettings) {
        if (Strings.hasText(clientSettings.proxyHost)) {
            try {
                // TODO: remove this leniency, these settings should exist together and be validated
                return ProxyConfiguration.builder()
                    .endpoint(
                        new URI(
                            clientSettings.protocol.toString(),
                            null,
                            clientSettings.proxyHost,
                            clientSettings.proxyPort,
                            null,
                            null,
                            null
                        )
                    )
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

    static ClientOverrideConfiguration buildOverrideConfiguration(Logger logger, Ec2ClientSettings clientSettings) {
        return ClientOverrideConfiguration.builder().retryPolicy(buildRetryPolicy(logger, clientSettings)).build();
    }

    // pkg private for tests
    static RetryPolicy buildRetryPolicy(Logger logger, Ec2ClientSettings clientSettings) {
        // Increase the number of retries in case of 5xx API responses.
        // Note that AWS SDK v2 introduced a concept of TokenBucketRetryCondition, which effectively limits retries for
        // APIs that have been failing continuously. It allocates tokens (default is 500), which means that once 500
        // retries fail for any API on a bucket, new retries will only be allowed once some retries are rejected.
        // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/conditions/TokenBucketRetryCondition.html
        RetryPolicy.Builder retryPolicy = RetryPolicy.builder().numRetries(10);
        return retryPolicy.build();
    }

    static AwsCredentialsProvider buildCredentials(Logger logger, Ec2ClientSettings clientSettings) {
        final AwsCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            logger.debug("Using default credentials provider");
            return DefaultCredentialsProvider.create();
        } else {
            logger.debug("Using basic key/secret credentials");
            return StaticCredentialsProvider.create(credentials);
        }
    }

    @Override
    public AmazonEc2ClientReference client() {
        final LazyInitializable<AmazonEc2ClientReference, OpenSearchException> clientReference = this.lazyClientReference.get();
        if (clientReference == null) {
            throw new IllegalStateException("Missing ec2 client configs");
        }
        return clientReference.getOrCompute();
    }

    /**
     * Refreshes the settings for the AmazonEC2 client. The new client will be build
     * using these new settings. The old client is usable until released. On release it
     * will be destroyed instead of being returned to the cache.
     */
    @Override
    public void refreshAndClearCache(Ec2ClientSettings clientSettings) {
        final LazyInitializable<AmazonEc2ClientReference, OpenSearchException> newClient = new LazyInitializable<>(
            () -> new AmazonEc2ClientReference(buildClient(clientSettings)),
            clientReference -> clientReference.incRef(),
            clientReference -> clientReference.decRef()
        );
        final LazyInitializable<AmazonEc2ClientReference, OpenSearchException> oldClient = this.lazyClientReference.getAndSet(newClient);
        if (oldClient != null) {
            oldClient.reset();
        }
    }

    @Override
    public void close() {
        final LazyInitializable<AmazonEc2ClientReference, OpenSearchException> clientReference = this.lazyClientReference.getAndSet(null);
        if (clientReference != null) {
            clientReference.reset();
        }
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
}
