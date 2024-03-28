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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import software.amazon.awssdk.core.internal.http.pipeline.stages.ApplyTransactionIdStage;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.RepositoryStats;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.OpenSearchMockAPIBasedRepositoryIntegTestCase;
import org.opensearch.repositories.s3.utils.AwsRequestSigner;
import org.opensearch.snapshots.mockstore.BlobStoreWrapper;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

import fixture.s3.S3HttpHandler;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
// Need to set up a new cluster for each test because cluster settings use randomized authentication settings
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class S3BlobStoreRepositoryTests extends OpenSearchMockAPIBasedRepositoryIntegTestCase {

    private final String region = "test-region";
    private String signerOverride;
    private String previousOpenSearchPathConf;

    @Override
    public void setUp() throws Exception {
        signerOverride = AwsRequestSigner.VERSION_FOUR_SIGNER.getName();
        previousOpenSearchPathConf = SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", "config"));
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (previousOpenSearchPathConf != null) {
            SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", previousOpenSearchPathConf));
        } else {
            SocketAccess.doPrivileged(() -> System.clearProperty("opensearch.path.conf"));
        }
        super.tearDown();
    }

    @Override
    protected String repositoryType() {
        return S3Repository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
            .put(S3Repository.CLIENT_NAME.getKey(), "test")
            // Don't cache repository data because some tests manually modify the repository data
            .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestS3RepositoryPlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/bucket", new S3StatsCollectorHttpHandler(new S3BlobStoreHttpHandler("bucket")));
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new S3StatsCollectorHttpHandler(new S3ErroneousHttpHandler(delegate, randomIntBetween(2, 3)));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "access");
        secureSettings.setString(S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), "secret");

        final Settings.Builder builder = Settings.builder()
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that verify an exact wait time
            .put(S3ClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), httpServerUrl())
            // Disable chunked encoding as it simplifies a lot the request parsing on the httpServer side
            .put(S3ClientSettings.DISABLE_CHUNKED_ENCODING.getConcreteSettingForNamespace("test").getKey(), true)
            // Disable request throttling because some random values in tests might generate too many failures for the S3 client
            .put(S3ClientSettings.USE_THROTTLE_RETRIES_SETTING.getConcreteSettingForNamespace("test").getKey(), false)
            .put(S3ClientSettings.PROXY_TYPE_SETTING.getConcreteSettingForNamespace("test").getKey(), ProxySettings.ProxyType.DIRECT)
            .put(super.nodeSettings(nodeOrdinal))
            .setSecureSettings(secureSettings);

        if (signerOverride != null) {
            builder.put(S3ClientSettings.SIGNER_OVERRIDE.getConcreteSettingForNamespace("test").getKey(), signerOverride);
        }

        builder.put(S3ClientSettings.REGION.getConcreteSettingForNamespace("test").getKey(), region);
        return builder.build();
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/10735")
    @Override
    public void testRequestStats() throws Exception {
        final String repository = createRepository(randomName());
        final String index = "index-no-merges";
        createIndex(
            index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        final long nbDocs = randomLongBetween(10_000L, 20_000L);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, "_doc", client(), (int) nbDocs)) {
            waitForDocs(nbDocs, indexer);
        }

        flushAndRefresh(index);
        ForceMergeResponse forceMerge = client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(1));
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        final String snapshot = "snapshot";
        assertSuccessfulSnapshot(
            client().admin().cluster().prepareCreateSnapshot(repository, snapshot).setWaitForCompletion(true).setIndices(index)
        );

        assertAcked(client().admin().indices().prepareDelete(index));

        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repository, snapshot).setWaitForCompletion(true));
        ensureGreen(index);
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repository, snapshot).get());

        final RepositoryStats repositoryStats = StreamSupport.stream(
            internalCluster().getInstances(RepositoriesService.class).spliterator(),
            false
        ).map(repositoriesService -> {
            try {
                return repositoriesService.repository(repository);
            } catch (RepositoryMissingException e) {
                return null;
            }
        }).filter(Objects::nonNull).map(Repository::stats).reduce(RepositoryStats::merge).get();

        Map<BlobStore.Metric, Map<String, Long>> extendedStats = repositoryStats.extendedStats;
        Map<String, Long> aggregatedStats = new HashMap<>();
        extendedStats.forEach((k, v) -> {
            if (k == BlobStore.Metric.RETRY_COUNT || k == BlobStore.Metric.REQUEST_SUCCESS || k == BlobStore.Metric.REQUEST_FAILURE) {
                for (Map.Entry<String, Long> entry : v.entrySet()) {
                    aggregatedStats.merge(entry.getKey(), entry.getValue(), Math::addExact);
                }
            }

        });
        final Map<String, Long> mockCalls = getMockRequestCounts();

        String assertionErrorMsg = String.format("SDK sent [%s] calls and handler measured [%s] calls", aggregatedStats, mockCalls);

        assertEquals(assertionErrorMsg, mockCalls, aggregatedStats);
    }

    /**
     * S3RepositoryPlugin that allows to disable chunked encoding and to set a low threshold between single upload and multipart upload.
     */
    public static class TestS3RepositoryPlugin extends S3RepositoryPlugin {

        public TestS3RepositoryPlugin(final Settings settings, final Path configPath) {
            super(settings, configPath);
        }

        @Override
        public List<Setting<?>> getSettings() {
            final List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(S3ClientSettings.DISABLE_CHUNKED_ENCODING);
            return settings;
        }

        @Override
        protected S3Repository createRepository(
            RepositoryMetadata metadata,
            NamedXContentRegistry registry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return new S3Repository(
                metadata,
                registry,
                service,
                clusterService,
                recoverySettings,
                null,
                null,
                null,
                null,
                null,
                false,
                null,
                null
            ) {

                @Override
                public BlobStore blobStore() {
                    return new BlobStoreWrapper(super.blobStore()) {
                        @Override
                        public BlobContainer blobContainer(final BlobPath path) {
                            return new S3BlobContainer(path, (S3BlobStore) delegate()) {
                                @Override
                                long getLargeBlobThresholdInBytes() {
                                    return ByteSizeUnit.MB.toBytes(1L);
                                }

                                @Override
                                void ensureMultiPartUploadSize(long blobSize) {}
                            };
                        }
                    };
                }
            };
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate an S3 endpoint")
    private class S3BlobStoreHttpHandler extends S3HttpHandler implements BlobStoreHttpHandler {

        S3BlobStoreHttpHandler(final String bucket) {
            super(bucket);
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            validateAuthHeader(exchange);
            super.handle(exchange);
        }

        private void validateAuthHeader(HttpExchange exchange) {
            final String authorizationHeaderV4 = exchange.getRequestHeaders().getFirst("Authorization");

            if ("AWS4SignerType".equals(signerOverride)) {
                assertThat(authorizationHeaderV4, containsString("aws4_request"));
            }
            if (authorizationHeaderV4 != null) {
                assertThat(authorizationHeaderV4, containsString("/" + region + "/s3/"));
            }
        }
    }

    /**
     * HTTP handler that injects random S3 service errors
     * <p>
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class S3ErroneousHttpHandler extends ErroneousHttpHandler {

        S3ErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            super(delegate, maxErrorsPerRequest);
        }

        @Override
        protected String requestUniqueId(final HttpExchange exchange) {
            // Amazon SDK client provides a unique ID per request
            return exchange.getRequestHeaders().getFirst(ApplyTransactionIdStage.HEADER_SDK_TRANSACTION_ID);
        }
    }

    /**
     * HTTP handler that tracks the number of requests performed against S3.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class S3StatsCollectorHttpHandler extends HttpStatsCollectorHandler {

        S3StatsCollectorHttpHandler(final HttpHandler delegate) {
            super(delegate);
        }

        @Override
        public void maybeTrack(final String request, Headers requestHeaders) {
            if (Regex.simpleMatch("GET /*?list-type=*", request)) {
                trackRequest("ListObjects");
            } else if (Regex.simpleMatch("GET /*/*", request)) {
                trackRequest("GetObject");
            } else if (isMultiPartUpload(request)) {
                trackRequest("PutMultipartObject");
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PutObject");
            } else if (Regex.simpleMatch("POST /*?delete*", request)) {
                trackRequest("DeleteObjects");
            }
        }

        private boolean isMultiPartUpload(String request) {
            return Regex.simpleMatch("POST /*/*?uploads", request)
                || Regex.simpleMatch("POST /*/*?*uploadId=*", request)
                || Regex.simpleMatch("PUT /*/*?*uploadId=*", request);
        }
    }
}
