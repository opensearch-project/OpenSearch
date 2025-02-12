/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;

public abstract class AbstractRemoteStoreMockRepositoryIntegTestCase extends AbstractSnapshotIntegTestCase {

    protected static final String REPOSITORY_NAME = "my-segment-repo-1";
    protected static final String TRANSLOG_REPOSITORY_NAME = "my-translog-repo-1";
    protected static final String INDEX_NAME = "remote-store-test-idx-1";

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

    public Settings buildRemoteStoreNodeAttributes(Path repoLocation, double ioFailureRate, String skipExceptionBlobList, long maxFailure) {
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
        String segmentRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            REPOSITORY_NAME
        );
        String translogRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            TRANSLOG_REPOSITORY_NAME
        );
        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            REPOSITORY_NAME
        );
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            REPOSITORY_NAME
        );

        return Settings.builder()
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, REPOSITORY_NAME)
            .put(segmentRepoTypeAttributeKey, "mock")
            .put(segmentRepoSettingsAttributeKeyPrefix + "location", repoLocation)
            .put(segmentRepoSettingsAttributeKeyPrefix + "random_control_io_exception_rate", ioFailureRate)
            .put(segmentRepoSettingsAttributeKeyPrefix + "skip_exception_on_verification_file", true)
            .put(segmentRepoSettingsAttributeKeyPrefix + "skip_exception_on_list_blobs", true)
            .put(segmentRepoSettingsAttributeKeyPrefix + "skip_exception_on_blobs", skipExceptionBlobList)
            .put(segmentRepoSettingsAttributeKeyPrefix + "max_failure_number", maxFailure)
            .put("node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, TRANSLOG_REPOSITORY_NAME)
            .put(translogRepoTypeAttributeKey, "mock")
            .put(translogRepoSettingsAttributeKeyPrefix + "location", repoLocation)
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, REPOSITORY_NAME)
            .put(stateRepoTypeAttributeKey, "mock")
            .put(stateRepoSettingsAttributeKeyPrefix + "location", repoLocation)
            .build();
    }

    protected void cleanupRepo() {
        logger.info("--> Cleanup the repository={}", REPOSITORY_NAME);
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).execute().actionGet();
        logger.info("--> Cleanup the repository={}", TRANSLOG_REPOSITORY_NAME);
        clusterAdmin().prepareCleanupRepository(TRANSLOG_REPOSITORY_NAME).execute().actionGet();
    }

    protected String setup(Path repoLocation, double ioFailureRate, String skipExceptionBlobList, long maxFailure) {
        return setup(repoLocation, ioFailureRate, skipExceptionBlobList, maxFailure, 0);
    }

    protected String setup(Path repoLocation, double ioFailureRate, String skipExceptionBlobList, long maxFailure, int replicaCount) {
        // The random_control_io_exception_rate setting ensures that 10-25% of all operations to remote store results in
        /// IOException. skip_exception_on_verification_file & skip_exception_on_list_blobs settings ensures that the
        // repository creation can happen without failure.
        Settings.Builder settings = Settings.builder()
            .put(buildRemoteStoreNodeAttributes(repoLocation, ioFailureRate, skipExceptionBlobList, maxFailure));

        if (randomBoolean()) {
            settings.put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT);
        }

        disableRepoConsistencyCheck("Remote Store Creates System Repository");

        internalCluster().startClusterManagerOnlyNode(settings.build());
        String dataNodeName = internalCluster().startDataOnlyNode(settings.build());
        internalCluster().startDataOnlyNodes(replicaCount, settings.build());
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

    protected IndexResponse indexSingleDoc() {
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
