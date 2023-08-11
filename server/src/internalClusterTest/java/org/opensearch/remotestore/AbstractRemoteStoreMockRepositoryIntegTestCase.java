/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.Before;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.test.FeatureFlagSetter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public abstract class AbstractRemoteStoreMockRepositoryIntegTestCase extends AbstractSnapshotIntegTestCase {

    protected static final String REPOSITORY_NAME = "my-segment-repo-1";
    protected static final String TRANSLOG_REPOSITORY_NAME = "my-translog-repo-1";
    protected static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE, "true").build();
    }

    @Before
    public void setup() {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        FeatureFlagSetter.set(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL);
        internalCluster().startClusterManagerOnlyNode(remoteStoreClusterSettings(REPOSITORY_NAME, TRANSLOG_REPOSITORY_NAME));
    }

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    protected void deleteRepo() {
        logger.info("--> Deleting the repository={}", REPOSITORY_NAME);
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
        logger.info("--> Deleting the repository={}", TRANSLOG_REPOSITORY_NAME);
        assertAcked(clusterAdmin().prepareDeleteRepository(TRANSLOG_REPOSITORY_NAME));
    }

    protected String setup(Path repoLocation, double ioFailureRate, String skipExceptionBlobList, long maxFailure) {
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
                // Skipping is required for metadata as it is part of recovery
                .put("skip_exception_on_blobs", skipExceptionBlobList)
                .put("max_failure_number", maxFailure)
        );
        logger.info("--> Creating repository={} at the path={}", TRANSLOG_REPOSITORY_NAME, repoLocation);
        createRepository(TRANSLOG_REPOSITORY_NAME, "mock", Settings.builder().put("location", repoLocation));

        String dataNodeName = internalCluster().startDataOnlyNodes(1).get(0);
        createIndex(INDEX_NAME);
        logger.info("--> Created index={}", INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        logger.info("--> Cluster is yellow with no initializing shards");
        ensureGreen(INDEX_NAME);
        logger.info("--> Cluster is green");
        return dataNodeName;
    }

    /**
     * Gets all segment files which starts with "_". For instance, _0.cfe, _o.cfs etc.
     *
     * @param location the path to location where segment files are being searched.
     * @return set of file names of all segment file or empty set if there was IOException thrown.
     */
    protected Set<String> getSegmentFiles(Path location) {
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

    protected void indexData(int numberOfIterations, boolean invokeFlush) {
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
