/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreRefreshListenerIT extends AbstractSnapshotIntegTestCase {

    private static final String REPOSITORY_NAME = "my-segment-repo-1";
    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .put(FeatureFlags.REMOTE_STORE, "true")
            .build();
    }

    @Before
    public void setup() {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode();
    }

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    private Settings remoteStoreIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    @After
    public void teardown() {
        logger.info("--> Deleting the repository={}", REPOSITORY_NAME);
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
    }

    public void testRemoteRefreshRetryOnFailure() throws Exception {

        Path location = randomRepoPath().toAbsolutePath();
        setup(location, randomDoubleBetween(0.1, 0.25, true), "metadata");

        // Here we are having flush/refresh after each iteration of indexing. However, the refresh will not always succeed
        // due to IOExceptions that are thrown while doing uploadBlobs.
        indexData(randomIntBetween(5, 10), randomBoolean());
        logger.info("--> Indexed data");

        // TODO - Once the segments stats api is available, we need to verify that there were failed upload attempts.
        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest()).get();
        assertEquals(1, response.getShards().length);

        String indexUuid = response.getShards()[0].getShardRouting().index().getUUID();
        Path segmentDataRepoPath = location.resolve(String.format(Locale.ROOT, "%s/0/segments/data", indexUuid));
        String segmentDataLocalPath = String.format(Locale.ROOT, "%s/indices/%s/0/index", response.getShards()[0].getDataPath(), indexUuid);

        logger.info("--> Verify that the segment files are same on local and repository eventually");
        // This can take time as the retry interval is exponential and maxed at 30s
        assertBusy(() -> {
            Set<String> filesInLocal = getSegmentFiles(location.getRoot().resolve(segmentDataLocalPath));
            Set<String> filesInRepo = getSegmentFiles(segmentDataRepoPath);
            assertTrue(filesInRepo.containsAll(filesInLocal));
        }, 60, TimeUnit.SECONDS);
    }

    private void setup(Path repoLocation, double ioFailureRate, String skipExceptionBlobList) {
        logger.info("--> Creating repository={} at the path={}", REPOSITORY_NAME, repoLocation);
        // The random_control_io_exception_rate setting ensures that 10-25% of all operations to remote store results in
        /// IOException. skip_exception_on_verification_file & skip_exception_on_list_blobs settings ensures that the
        // repository creation can happen without failure.
        createRepository(
            REPOSITORY_NAME,
            "mock",
            Settings.builder()
                .put("location", repoLocation)
                .put("random_control_io_exception_rate", ioFailureRate)
                .put("skip_exception_on_verification_file", true)
                .put("skip_exception_on_list_blobs", true)
                .put("max_failure_number", Long.MAX_VALUE)
        );

        internalCluster().startDataOnlyNodes(1);
        createIndex(INDEX_NAME);
        logger.info("--> Created index={}", INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        logger.info("--> Cluster is yellow with no initializing shards");
        ensureGreen(INDEX_NAME);
        logger.info("--> Cluster is green");
    }

    /**
     * Gets all segment files which starts with "_". For instance, _0.cfe, _o.cfs etc.
     *
     * @param location the path to location where segment files are being searched.
     * @return set of file names of all segment file or empty set if there was IOException thrown.
     */
    private Set<String> getSegmentFiles(Path location) {
        try {
            return Arrays.stream(FileSystemUtils.files(location))
                .filter(path -> path.getFileName().toString().startsWith("_"))
                .map(path -> path.getFileName().toString())
                .map(this::getLocalSegmentFilename)
                .collect(Collectors.toSet());
        } catch (IOException exception) {
            logger.error("Exception occurred while getting segment files", exception);
        }
        return Collections.emptySet();
    }

    private String getLocalSegmentFilename(String remoteFilename) {
        return remoteFilename.split(RemoteSegmentStoreDirectory.SEGMENT_NAME_UUID_SEPARATOR)[0];
    }

    private IndexResponse indexSingleDoc() {
        return client().prepareIndex(INDEX_NAME)
            .setId(UUIDs.randomBase64UUID())
            .setSource(randomAlphaOfLength(5), randomAlphaOfLength(5))
            .get();
    }

    private void indexData(int numberOfIterations, boolean invokeFlush) {
        logger.info("--> Indexing data for {} iterations with flush={}", numberOfIterations, invokeFlush);
        for (int i = 0; i < numberOfIterations; i++) {
            int numberOfOperations = randomIntBetween(1, 5);
            logger.info("--> Indexing {} operations in iteration #{}", numberOfOperations, i);
            for (int j = 0; j < numberOfOperations; j++) {
                indexSingleDoc();
            }
            if (invokeFlush) {
                flush(INDEX_NAME);
            } else {
                refresh(INDEX_NAME);
            }
        }
    }
}
