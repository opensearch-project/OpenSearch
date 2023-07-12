/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.junit.Before;
import org.opensearch.cli.SuppressForbidden;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.Executors;

public class S3AsyncServiceTests extends OpenSearchTestCase implements ConfigPathSupport {

    @Override
    @Before
    @SuppressForbidden(reason = "Need to set opensearch.path.conf for async client")
    public void setUp() throws Exception {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        super.setUp();
    }

    public void testCachedClientsAreReleased() {
        final S3AsyncService s3AsyncService = new S3AsyncService(configPath());
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
        final AmazonAsyncS3Reference reference = SocketAccess.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer)
        );
        reference.close();
        s3AsyncService.close();
        final AmazonAsyncS3Reference referenceReloaded = SocketAccess.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer)
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
        final AmazonAsyncS3Reference reference = SocketAccess.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer)
        );
        reference.close();
        s3AsyncService.close();
        final AmazonAsyncS3Reference referenceReloaded = SocketAccess.doPrivileged(
            () -> s3AsyncService.client(metadata1, asyncExecutorContainer, asyncExecutorContainer)
        );
        assertNotSame(referenceReloaded, reference);
        referenceReloaded.close();
        s3AsyncService.close();
        final S3ClientSettings clientSettingsReloaded = s3AsyncService.settings(metadata1);
        assertNotSame(clientSettings, clientSettingsReloaded);
    }
}
