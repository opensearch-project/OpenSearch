/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import com.sun.net.httpserver.HttpServer;

import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.s3.utils.AwsRequestSigner;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import fixture.s3.DelayedS3HttpHandler;

import static org.hamcrest.Matchers.greaterThan;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
@ThreadLeakFilters(filters = EventLoopThreadFilter.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class S3SyncClientRequestTimeoutIT extends OpenSearchIntegTestCase {

    private static final int DELAY_MILLIS = 15000;
    private static final String REQUEST_TIMEOUT = "3s";

    private static HttpServer httpServer;
    private static DelayedS3HttpHandler delayedHandler;
    private String previousOpenSearchPathConf;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.setExecutor(Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }));
        httpServer.start();
        delayedHandler = new DelayedS3HttpHandler("bucket", DELAY_MILLIS, "PUT", "/snap-");
        httpServer.createContext("/bucket", delayedHandler);
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
    }

    @Override
    public void setUp() throws Exception {
        previousOpenSearchPathConf = AccessController.doPrivileged(() -> System.setProperty("opensearch.path.conf", "config"));
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (previousOpenSearchPathConf != null) {
            AccessController.doPrivileged(() -> System.setProperty("opensearch.path.conf", previousOpenSearchPathConf));
        } else {
            AccessController.doPrivileged(() -> System.clearProperty("opensearch.path.conf"));
        }
        super.tearDown();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TimeoutTestS3RepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "access");
        secureSettings.setString(S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "secret");

        InetSocketAddress address = httpServer.getAddress();
        String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();

        return Settings.builder()
            .put(S3ClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint)
            .put(S3ClientSettings.DISABLE_CHUNKED_ENCODING.getConcreteSettingForNamespace("test").getKey(), true)
            .put(S3ClientSettings.USE_THROTTLE_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), false)
            .put(S3ClientSettings.PROXY_TYPE_SETTING.getConcreteSettingForNamespace("test").getKey(), ProxySettings.ProxyType.DIRECT)
            .put(S3ClientSettings.REGION.getConcreteSettingForNamespace("test").getKey(), "test-region")
            .put(
                S3ClientSettings.SIGNER_OVERRIDE.getConcreteSettingForNamespace("test").getKey(),
                AwsRequestSigner.VERSION_FOUR_SIGNER.getName()
            )
            .put(S3ClientSettings.REQUEST_TIMEOUT_SETTING.getConcreteSettingForNamespace("test").getKey(), REQUEST_TIMEOUT)
            .put(S3ClientSettings.MAX_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), 1)
            .put(super.nodeSettings(nodeOrdinal))
            .setSecureSettings(secureSettings)
            .build();
    }

    public void testSyncClientTimesOutOnSlowRepository() throws Exception {
        delayedHandler.resetDelayedRequestCount();

        client().admin()
            .cluster()
            .putRepository(
                new PutRepositoryRequest("test-repo").type("s3")
                    .settings(
                        Settings.builder()
                            .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                            .put(S3Repository.CLIENT_NAME.getKey(), "test")
                    )
                    .verify(false)
            )
            .get();

        final String index = "test-index";
        createIndex(
            index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        client().prepareIndex(index).setSource("field", "value").get();
        flushAndRefresh(index);

        try {
            CreateSnapshotResponse response = client().admin()
                .cluster()
                .prepareCreateSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setIndices(index)
                .get();

            SnapshotState state = response.getSnapshotInfo().state();
            assertTrue("Snapshot should fail due to request timeout, but got state: " + state, state == SnapshotState.FAILED);
        } catch (Exception e) {
            Throwable cause = e;
            boolean foundTimeout = false;
            while (cause != null) {
                if (cause.getMessage() != null && cause.getMessage().contains("timeout")) {
                    foundTimeout = true;
                    break;
                }
                cause = cause.getCause();
            }
            assertTrue("Expected timeout-related exception but got: " + e.getMessage(), foundTimeout);
        }

        assertThat("Expected requests to the delayed S3 handler", delayedHandler.getDelayedRequestCount(), greaterThan(0));
    }

    /**
     * Plugin that provides the S3 repository for testing.
     */
    public static class TimeoutTestS3RepositoryPlugin extends S3RepositoryPlugin {
        public TimeoutTestS3RepositoryPlugin(final Settings settings, final Path configPath) {
            super(
                settings,
                configPath,
                new S3Service(configPath, Executors.newSingleThreadScheduledExecutor()),
                new S3AsyncService(configPath, Executors.newSingleThreadScheduledExecutor())
            );
        }

        @Override
        public List<Setting<?>> getSettings() {
            final List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(S3ClientSettings.DISABLE_CHUNKED_ENCODING);
            settings.add(S3ClientSettings.REQUEST_TIMEOUT_SETTING);
            return settings;
        }

        @Override
        public void close() throws IOException {
            super.close();
            Stream.of(service.getClientExecutorService(), s3AsyncService.getClientExecutorService())
                .forEach(e -> assertTrue(ThreadPool.terminate(e, 5, TimeUnit.SECONDS)));
        }
    }
}
