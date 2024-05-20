/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequest;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.remote.RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreRefreshListenerMultipartIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockFsRepositoryPlugin.class);
    }

    @Override
    public Settings buildRemoteStoreNodeAttributes(Path repoLocation, double ioFailureRate, String skipExceptionBlobList, long maxFailure) {
        Settings settings = super.buildRemoteStoreNodeAttributes(repoLocation, ioFailureRate, skipExceptionBlobList, maxFailure);
        String segmentRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            REPOSITORY_NAME
        );
        String translogRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            TRANSLOG_REPOSITORY_NAME
        );

        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            REPOSITORY_NAME
        );

        return Settings.builder()
            .put(settings)
            .put(segmentRepoTypeAttributeKey, MockFsRepositoryPlugin.TYPE)
            .put(translogRepoTypeAttributeKey, MockFsRepositoryPlugin.TYPE)
            .put(stateRepoTypeAttributeKey, MockFsRepositoryPlugin.TYPE)
            .build();
    }

    public void testRemoteRefreshSegmentUploadTimeout() throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        setup(location, randomDoubleBetween(0.1, 0.15, true), "metadata", 10L);

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), false))
            .setPersistentSettings(
                Settings.builder()
                    .put(RemoteStoreSettings.CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(1))
            )
            .get();

        // Here we are having flush/refresh after each iteration of indexing. However, the refresh will not always succeed
        // due to IOExceptions that are thrown while doing uploadBlobs.
        indexData(randomIntBetween(5, 10), randomBoolean());
        logger.info("--> Indexed data");
        logger.info("--> Verify that the segment upload fails");
        try {
            assertBusy(() -> {
                RemoteStoreStatsResponse remoteStoreStatsResponse = client().admin()
                    .cluster()
                    .remoteStoreStats(new RemoteStoreStatsRequest())
                    .get();
                Arrays.asList(remoteStoreStatsResponse.getRemoteStoreStats()).forEach(remoteStoreStats -> {
                    assertTrue(remoteStoreStats.getSegmentStats().totalUploadsFailed > 10);
                });
            }, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        cleanupRepo();
    }
}
