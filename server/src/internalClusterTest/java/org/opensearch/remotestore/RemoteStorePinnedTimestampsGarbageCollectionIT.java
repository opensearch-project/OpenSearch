/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.opensearch.index.IndexSettings.INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStorePinnedTimestampsGarbageCollectionIT extends RemoteStoreBaseIntegTestCase {
    static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), false)
            .build();
    }

    private void keepPinnedTimestampSchedulerUpdated() throws InterruptedException {
        long currentTime = System.currentTimeMillis();
        int maxRetry = 10;
        while (maxRetry > 0 && RemoteStorePinnedTimestampService.getPinnedTimestamps().v1() <= currentTime) {
            Thread.sleep(1000);
            maxRetry--;
        }
    }

    ActionListener<Void> noOpActionListener = new ActionListener<>() {
        @Override
        public void onResponse(Void unused) {}

        @Override
        public void onFailure(Exception e) {}
    };

    public void testLiveIndexNoPinnedTimestamps() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.getKey(), 0)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        int numDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numDocs; i++) {
            keepPinnedTimestampSchedulerUpdated();
            indexSingleDoc(INDEX_NAME, true);
        }

        String translogPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings());
        String shardDataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            DATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogDataPath = Path.of(translogRepoPath + "/" + shardDataPath + "/1");
        String shardMetadataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            METADATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogMetadataPath = Path.of(translogRepoPath + "/" + shardMetadataPath);

        assertBusy(() -> {
            List<Path> metadataFiles = Files.list(translogMetadataPath).collect(Collectors.toList());
            assertEquals(1, metadataFiles.size());

            verifyTranslogDataFileCount(metadataFiles, translogDataPath);
        });
    }

    public void testLiveIndexNoPinnedTimestampsWithExtraGenSettingWithinLimit() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.getKey(), 10)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        int numDocs = randomIntBetween(5, 9);
        for (int i = 0; i < numDocs; i++) {
            keepPinnedTimestampSchedulerUpdated();
            indexSingleDoc(INDEX_NAME, true);
        }

        String translogPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings());
        String shardDataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            DATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogDataPath = Path.of(translogRepoPath + "/" + shardDataPath + "/1");
        String shardMetadataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            METADATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogMetadataPath = Path.of(translogRepoPath + "/" + shardMetadataPath);

        assertBusy(() -> {
            List<Path> metadataFiles = Files.list(translogMetadataPath).collect(Collectors.toList());
            assertEquals(numDocs + 1, metadataFiles.size());

            verifyTranslogDataFileCount(metadataFiles, translogDataPath);
        });
    }

    public void testLiveIndexNoPinnedTimestampsWithExtraGenSetting() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.getKey(), 3)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        int numDocs = 5;
        for (int i = 0; i < numDocs; i++) {
            keepPinnedTimestampSchedulerUpdated();
            indexSingleDoc(INDEX_NAME, true);
        }

        String translogPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings());
        String shardDataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            DATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogDataPath = Path.of(translogRepoPath + "/" + shardDataPath + "/1");
        String shardMetadataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            METADATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogMetadataPath = Path.of(translogRepoPath + "/" + shardMetadataPath);

        assertBusy(() -> {
            List<Path> metadataFiles = Files.list(translogMetadataPath).collect(Collectors.toList());
            assertEquals(3, metadataFiles.size());

            verifyTranslogDataFileCount(metadataFiles, translogDataPath);
        });
    }

    public void testLiveIndexWithPinnedTimestamps() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.getKey(), 0)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        int numDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numDocs; i++) {
            keepPinnedTimestampSchedulerUpdated();
            indexSingleDoc(INDEX_NAME, true);
            if (i == 2) {
                RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.timeValueMinutes(1));
                remoteStorePinnedTimestampService.pinTimestamp(System.currentTimeMillis(), "xyz", noOpActionListener);
                RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
            }
        }

        String translogPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings());
        String shardDataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            DATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogDataPath = Path.of(translogRepoPath + "/" + shardDataPath + "/1");
        String shardMetadataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            METADATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogMetadataPath = Path.of(translogRepoPath + "/" + shardMetadataPath);

        assertBusy(() -> {
            List<Path> metadataFiles = Files.list(translogMetadataPath).collect(Collectors.toList());
            assertEquals(2, metadataFiles.size());

            verifyTranslogDataFileCount(metadataFiles, translogDataPath);
        });
    }

    public void testIndexDeletionNoPinnedTimestamps() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.getKey(), 0)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        int numDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numDocs; i++) {
            keepPinnedTimestampSchedulerUpdated();
            indexSingleDoc(INDEX_NAME, true);
        }

        String translogPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings());
        String shardDataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            DATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogDataPath = Path.of(translogRepoPath + "/" + shardDataPath + "/1");
        String shardMetadataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            METADATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogMetadataPath = Path.of(translogRepoPath + "/" + shardMetadataPath);

        assertBusy(() -> {
            List<Path> metadataFiles = Files.list(translogMetadataPath).collect(Collectors.toList());
            assertEquals(1, metadataFiles.size());

            verifyTranslogDataFileCount(metadataFiles, translogDataPath);
        });

        keepPinnedTimestampSchedulerUpdated();
        client().admin().indices().prepareDelete(INDEX_NAME).get();

        assertBusy(() -> {
            assertEquals(0, Files.list(translogMetadataPath).collect(Collectors.toList()).size());
            assertEquals(0, Files.list(translogDataPath).collect(Collectors.toList()).size());
        });
    }

    public void testIndexDeletionWithPinnedTimestamps() throws Exception {
        prepareCluster(1, 1, Settings.EMPTY);
        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.getKey(), 0)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        int numDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numDocs; i++) {
            keepPinnedTimestampSchedulerUpdated();
            indexSingleDoc(INDEX_NAME, true);
            if (i == 2) {
                RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.timeValueMinutes(1));
                remoteStorePinnedTimestampService.pinTimestamp(System.currentTimeMillis(), "xyz", noOpActionListener);
                RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
            }
        }

        String translogPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings());
        String shardDataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            DATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogDataPath = Path.of(translogRepoPath + "/" + shardDataPath + "/1");
        String shardMetadataPath = getShardLevelBlobPath(
            client(),
            INDEX_NAME,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            METADATA,
            translogPathFixedPrefix
        ).buildAsString();
        Path translogMetadataPath = Path.of(translogRepoPath + "/" + shardMetadataPath);

        assertBusy(() -> {
            List<Path> metadataFiles = Files.list(translogMetadataPath).collect(Collectors.toList());
            assertEquals(2, metadataFiles.size());

            verifyTranslogDataFileCount(metadataFiles, translogDataPath);
        }, 30, TimeUnit.SECONDS);

        keepPinnedTimestampSchedulerUpdated();
        client().admin().indices().prepareDelete(INDEX_NAME).get();

        assertBusy(() -> {
            List<Path> metadataFiles = Files.list(translogMetadataPath).collect(Collectors.toList());
            assertEquals(1, metadataFiles.size());

            verifyTranslogDataFileCount(metadataFiles, translogDataPath);
        });
    }

    private void verifyTranslogDataFileCount(List<Path> metadataFiles, Path translogDataPath) throws IOException {
        List<String> mdFiles = metadataFiles.stream().map(p -> p.getFileName().toString()).collect(Collectors.toList());
        Set<Long> generations = new HashSet<>();
        for (String mdFile : mdFiles) {
            Tuple<Long, Long> minMaxGen = TranslogTransferMetadata.getMinMaxTranslogGenerationFromFilename(mdFile);
            generations.addAll(LongStream.rangeClosed(minMaxGen.v1(), minMaxGen.v2()).boxed().collect(Collectors.toList()));
        }
        assertEquals(generations.size() * 2, Files.list(translogDataPath).collect(Collectors.toList()).size());
    }
}
