/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.opensearch.index.store.RemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH;
import static org.opensearch.test.RemoteStoreTestUtils.createMetadataFileBytes;
import static org.opensearch.test.RemoteStoreTestUtils.getDummyMetadata;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseRemoteSegmentStoreDirectoryTests extends IndexShardTestCase {

    protected RemoteDirectory remoteDataDirectory;
    protected RemoteDirectory remoteMetadataDirectory;
    protected RemoteStoreMetadataLockManager mdLockManager;
    protected RemoteSegmentStoreDirectory remoteSegmentStoreDirectory;
    protected TestUploadListener testUploadTracker;
    protected IndexShard indexShard;
    protected SegmentInfos segmentInfos;
    protected ThreadPool threadPool;

    protected String metadataFilename = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(12, 23, 34, 1, 1, "node-1");

    protected String metadataFilenameDup = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        23,
        34,
        2,
        1,
        "node-2"
    );
    protected String metadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(12, 13, 34, 1, 1, "node-1");
    protected String metadataFilename3 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(10, 38, 34, 1, 1, "node-1");
    protected String metadataFilename4 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(10, 36, 34, 1, 1, "node-1");

    public void setupRemoteSegmentStoreDirectory() throws IOException {
        remoteDataDirectory = mock(RemoteDirectory.class);
        remoteMetadataDirectory = mock(RemoteDirectory.class);
        mdLockManager = mock(RemoteStoreMetadataLockManager.class);
        threadPool = mock(ThreadPool.class);
        testUploadTracker = new TestUploadListener();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();

        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());
        remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDataDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            indexShard.shardId()
        );
        try (Store store = indexShard.store()) {
            segmentInfos = store.readLastCommittedSegmentsInfo();
        }

        when(threadPool.executor(ThreadPool.Names.REMOTE_PURGE)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.SAME)).thenReturn(executorService);
    }

    protected Map<String, Map<String, String>> populateMetadata() throws IOException {
        List<String> metadataFiles = new ArrayList<>();

        metadataFiles.add(metadataFilename);
        metadataFiles.add(metadataFilename2);
        metadataFiles.add(metadataFilename3);

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(List.of(metadataFilename));
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                Integer.MAX_VALUE
            )
        ).thenReturn(metadataFiles);

        Map<String, Map<String, String>> metadataFilenameContentMapping = Map.of(
            metadataFilename,
            getDummyMetadata("_0", 1),
            metadataFilename2,
            getDummyMetadata("_0", 1),
            metadataFilename3,
            getDummyMetadata("_0", 1)
        );

        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenAnswer(
            I -> createMetadataFileBytes(
                metadataFilenameContentMapping.get(metadataFilename),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename2)).thenAnswer(
            I -> createMetadataFileBytes(
                metadataFilenameContentMapping.get(metadataFilename2),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename3)).thenAnswer(
            I -> createMetadataFileBytes(
                metadataFilenameContentMapping.get(metadataFilename3),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );

        return metadataFilenameContentMapping;
    }

    @After
    public void tearDown() throws Exception {
        indexShard.close("test tearDown", true, false);
        super.tearDown();
    }

}
