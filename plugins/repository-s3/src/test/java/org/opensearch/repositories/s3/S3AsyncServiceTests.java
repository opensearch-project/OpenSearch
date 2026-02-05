/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import org.opensearch.cli.SuppressForbidden;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

import io.netty.channel.nio.NioEventLoopGroup;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3AsyncServiceTests extends OpenSearchTestCase implements ConfigPathSupport {

    @Override
    @Before
    @SuppressForbidden(reason = "Need to set opensearch.path.conf for async client")
    public void setUp() throws Exception {
        AccessController.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        super.setUp();
    }

    public void testCachedClientsAreReleased() {
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());
        final Settings settings = Settings.builder()
            .put("endpoint", "http://first")
            .put("region", "us-east-2")
            .put(S3Repository.S3_ASYNC_HTTP_CLIENT_TYPE.getKey(), S3Repository.NETTY_ASYNC_HTTP_CLIENT_TYPE)
            .build();

        final Settings crtSettings = Settings.builder()
            .put("endpoint", "http://first")
            .put("region", "us-east-2")
            .put(S3Repository.S3_ASYNC_HTTP_CLIENT_TYPE.getKey(), S3Repository.CRT_ASYNC_HTTP_CLIENT_TYPE)
            .build();

        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final RepositoryMetadata metadata2 = new RepositoryMetadata("second", "s3", settings);

        final RepositoryMetadata metadata3 = new RepositoryMetadata("second", "s3", crtSettings);
        final RepositoryMetadata metadata4 = new RepositoryMetadata("second", "s3", crtSettings);
        final AsyncExecutorContainer asyncExecutorContainer = new AsyncExecutorContainer(
            Executors.newSingleThreadExecutor(),
            Executors.newSingleThreadExecutor(),
            new AsyncTransferEventLoopGroup(1)
        );
        final S3ClientSettings clientSettings = s3AsyncService.settings(metadata2);
        final S3ClientSettings otherClientSettings = s3AsyncService.settings(metadata2);
        assertSame(clientSettings, otherClientSettings);
        final AmazonAsyncS3Reference reference = AccessController.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer, asyncExecutorContainer)
        );

        final AmazonAsyncS3Reference reference2 = AccessController.doPrivileged(
            () -> s3AsyncService.client(metadata2, asyncExecutorContainer, asyncExecutorContainer, asyncExecutorContainer)
        );

        final AmazonAsyncS3Reference reference3 = AccessController.doPrivileged(
            () -> s3AsyncService.client(metadata3, asyncExecutorContainer, asyncExecutorContainer, asyncExecutorContainer)
        );

        final AmazonAsyncS3Reference reference4 = AccessController.doPrivileged(
            () -> s3AsyncService.client(metadata4, asyncExecutorContainer, asyncExecutorContainer, asyncExecutorContainer)
        );

        assertSame(reference, reference2);
        assertSame(reference3, reference4);
        assertNotSame(reference, reference3);

        reference.close();
        s3AsyncService.close();
        final AmazonAsyncS3Reference referenceReloaded = AccessController.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer, asyncExecutorContainer)
        );
        assertNotSame(referenceReloaded, reference);
        referenceReloaded.close();
        s3AsyncService.close();
        final S3ClientSettings clientSettingsReloaded = s3AsyncService.settings(metadata1);
        assertNotSame(clientSettings, clientSettingsReloaded);
    }

    public void testCachedClientsWithCredentialsAreReleased() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.role_arn", "role");
        final Map<String, S3ClientSettings> defaults = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).put("s3.client.default.identity_token_file", "file").build(),
            configPath()
        );
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());
        s3AsyncService.refreshAndClearCache(defaults);
        final Settings settings = Settings.builder().put("endpoint", "http://first").put("region", "us-east-2").build();
        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final RepositoryMetadata metadata2 = new RepositoryMetadata("second", "s3", settings);
        final AsyncExecutorContainer asyncExecutorContainer = new AsyncExecutorContainer(
            Executors.newSingleThreadExecutor(),
            Executors.newSingleThreadExecutor(),
            new AsyncTransferEventLoopGroup(1)
        );
        final S3ClientSettings clientSettings = s3AsyncService.settings(metadata2);
        final S3ClientSettings otherClientSettings = s3AsyncService.settings(metadata2);
        assertSame(clientSettings, otherClientSettings);
        final AmazonAsyncS3Reference reference = AccessController.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer, asyncExecutorContainer)
        );
        reference.close();
        s3AsyncService.close();
        final AmazonAsyncS3Reference referenceReloaded = AccessController.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer, asyncExecutorContainer)
        );
        assertNotSame(referenceReloaded, reference);
        referenceReloaded.close();
        s3AsyncService.close();
        final S3ClientSettings clientSettingsReloaded = s3AsyncService.settings(metadata1);
        assertNotSame(clientSettings, clientSettingsReloaded);
    }

    public void testBuildHttpClientWithNetty() {
        final int port = randomIntBetween(10, 1080);
        final String userName = randomAlphaOfLength(10);
        final String password = randomAlphaOfLength(10);
        final String proxyType = randomFrom("http", "https", "socks");
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", userName);
        secureSettings.setString("s3.client.default.proxy.password", password);

        final Settings settings = Settings.builder()
            .put("endpoint", "http://first")
            .put("region", "us-east-2")
            .put("s3.client.default.proxy.type", proxyType)
            .put("s3.client.default.proxy.host", randomFrom("127.0.0.10"))
            .put("s3.client.default.proxy.port", randomFrom(port))
            .setSecureSettings(secureSettings)
            .build();
        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final S3ClientSettings clientSettings = s3AsyncService.settings(metadata1);

        AsyncTransferEventLoopGroup eventLoopGroup = mock(AsyncTransferEventLoopGroup.class);
        when(eventLoopGroup.getEventLoopGroup()).thenReturn(mock(NioEventLoopGroup.class));

        SdkAsyncHttpClient asyncClient = S3AsyncService.buildHttpClient(
            clientSettings,
            eventLoopGroup,
            S3Repository.NETTY_ASYNC_HTTP_CLIENT_TYPE
        );
        assertNotNull(asyncClient);
        assertTrue(asyncClient instanceof NettyNioAsyncHttpClient);
        verify(eventLoopGroup).getEventLoopGroup();
    }

    public void testBuildHttpClientWithCRT() {
        final int port = randomIntBetween(10, 1080);
        final String userName = randomAlphaOfLength(10);
        final String password = randomAlphaOfLength(10);
        final String proxyType = randomFrom("http", "https", "socks");
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", userName);
        secureSettings.setString("s3.client.default.proxy.password", password);

        final Settings settings = Settings.builder()
            .put("endpoint", "http://first")
            .put("region", "us-east-2")
            .put("s3.client.default.proxy.type", proxyType)
            .put("s3.client.default.proxy.host", randomFrom("127.0.0.10"))
            .put("s3.client.default.proxy.port", randomFrom(port))
            .setSecureSettings(secureSettings)
            .build();

        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final S3ClientSettings clientSettings = s3AsyncService.settings(metadata1);

        AsyncTransferEventLoopGroup eventLoopGroup = mock(AsyncTransferEventLoopGroup.class);
        when(eventLoopGroup.getEventLoopGroup()).thenReturn(mock(NioEventLoopGroup.class));

        SdkAsyncHttpClient asyncClient = S3AsyncService.buildHttpClient(
            clientSettings,
            eventLoopGroup,
            S3Repository.CRT_ASYNC_HTTP_CLIENT_TYPE
        );
        assertNotNull(asyncClient);
        assertTrue(asyncClient instanceof AwsCrtAsyncHttpClient);
    }

    public void testResolveEndpointOverrideAbsentWhenEndpointNotProvided() {
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());
        final Settings repoSettings = Settings.builder().put("region", "us-east-1").build();
        final RepositoryMetadata metadata = new RepositoryMetadata("no-endpoint", "s3", repoSettings);

        final S3ClientSettings clientSettings = s3AsyncService.settings(metadata);
        final Optional<URI> override = s3AsyncService.resolveEndpointOverride(clientSettings);
        assertTrue("Expected no endpoint override when endpoint setting is absent", override.isEmpty());
    }

    public void testResolveEndpointOverrideAddsSchemeWhenMissing() {
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());
        final Settings repoSettings = Settings.builder().put("region", "us-east-1").put("endpoint", "s3.us-east-1.amazonaws.com").build();
        final RepositoryMetadata metadata = new RepositoryMetadata("endpoint-no-scheme", "s3", repoSettings);

        final S3ClientSettings clientSettings = s3AsyncService.settings(metadata);
        final Optional<URI> override = s3AsyncService.resolveEndpointOverride(clientSettings);
        assertTrue("Expected endpoint override to be present when endpoint setting is provided", override.isPresent());
        assertEquals("https://s3.us-east-1.amazonaws.com", override.get().toString());
    }

    public void testResolveEndpointOverridePreservesExplicitScheme() {
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());
        final Settings repoSettings = Settings.builder().put("region", "us-east-1").put("endpoint", "http://localhost:9000").build();
        final RepositoryMetadata metadata = new RepositoryMetadata("endpoint-with-scheme", "s3", repoSettings);

        final S3ClientSettings clientSettings = s3AsyncService.settings(metadata);
        final Optional<URI> override = s3AsyncService.resolveEndpointOverride(clientSettings);
        assertTrue("Expected endpoint override to be present when endpoint has explicit scheme", override.isPresent());
        assertEquals("http://localhost:9000", override.get().toString());
    }
}
