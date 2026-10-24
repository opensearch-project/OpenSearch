/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.sun.net.httpserver.HttpServer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.instanceOf;

public class S3ApiCallTimeoutTests extends AbstractS3RepositoryTestCase {

    private static final int MAX_RETRIES = 100;

    public void testSyncApiCallTimeoutStopsRetries() throws Exception {
        final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        final ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
        try (FailingS3Endpoint endpoint = new FailingS3Endpoint()) {
            final S3ClientSettings settings = clientSettings();
            try (
                S3Client client = AccessController.doPrivileged(
                    () -> S3Client.builder()
                        .region(Region.US_EAST_1)
                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret")))
                        .endpointOverride(endpoint.uri())
                        .forcePathStyle(true)
                        .httpClientBuilder(S3Service.buildHttpClient(settings))
                        .overrideConfiguration(S3Service.buildOverrideConfiguration(settings, timeoutExecutor))
                        .build()
                )
            ) {
                client.headBucket(b -> b.bucket("bucket").overrideConfiguration(o -> o.apiCallTimeout(Duration.ofSeconds(10))));
                endpoint.failRequests.set(true);
                final Future<?> request = requestExecutor.submit(() -> client.headBucket(b -> b.bucket("bucket")));
                try {
                    assertTimeoutStopsRetries(request, endpoint);
                } finally {
                    request.cancel(true);
                }
            }
        } finally {
            terminate(requestExecutor);
            terminate(timeoutExecutor);
        }
    }

    public void testAsyncApiCallTimeoutStopsRetries() throws Exception {
        final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        try (FailingS3Endpoint endpoint = new FailingS3Endpoint()) {
            final S3ClientSettings settings = clientSettings();
            try (
                S3AsyncClient client = AccessController.doPrivileged(
                    () -> S3AsyncClient.builder()
                        .region(Region.US_EAST_1)
                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret")))
                        .endpointOverride(endpoint.uri())
                        .forcePathStyle(true)
                        .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(2))
                        .overrideConfiguration(S3AsyncService.buildOverrideConfiguration(settings, timeoutExecutor))
                        .build()
                )
            ) {
                client.headBucket(b -> b.bucket("bucket").overrideConfiguration(o -> o.apiCallTimeout(Duration.ofSeconds(10))))
                    .get(15, TimeUnit.SECONDS);
                endpoint.failRequests.set(true);
                final Future<?> request = client.headBucket(b -> b.bucket("bucket"));
                try {
                    assertTimeoutStopsRetries(request, endpoint);
                } finally {
                    request.cancel(true);
                }
            }
        } finally {
            terminate(timeoutExecutor);
        }
    }

    private S3ClientSettings clientSettings() {
        return S3ClientSettings.load(
            Settings.builder()
                .put("s3.client.default.api_call_timeout", "2s")
                .put("s3.client.default.request_timeout", "10s")
                .put("s3.client.default.max_retries", MAX_RETRIES)
                .build(),
            configPath()
        ).get("default");
    }

    private void assertTimeoutStopsRetries(Future<?> request, FailingS3Endpoint endpoint) throws Exception {
        final ExecutionException failure = expectThrows(ExecutionException.class, () -> request.get(15, TimeUnit.SECONDS));
        assertThat(failure.getCause(), instanceOf(ApiCallTimeoutException.class));
        assertTrue("expected at least one retry", endpoint.failedRequests.get() > 1);
        assertTrue("the total timeout must stop retries before they are exhausted", endpoint.failedRequests.get() < MAX_RETRIES + 1);
    }

    @SuppressForbidden(reason = "uses a local HTTP server to exercise SDK retries")
    private static class FailingS3Endpoint implements AutoCloseable {

        private final HttpServer server;
        private final AtomicBoolean failRequests = new AtomicBoolean();
        private final AtomicInteger failedRequests = new AtomicInteger();

        private FailingS3Endpoint() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", exchange -> {
                try {
                    final boolean fail = failRequests.get();
                    if (fail) {
                        failedRequests.incrementAndGet();
                    }
                    exchange.sendResponseHeaders(fail ? 503 : 200, -1);
                } finally {
                    exchange.close();
                }
            });
            server.start();
        }

        private URI uri() {
            final InetSocketAddress address = server.getAddress();
            return URI.create("http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort());
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
