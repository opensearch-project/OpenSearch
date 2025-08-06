/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.recovery.RecoveryState;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;

public class ShardLuceneUtils {

    static boolean localDirectoryContains(Directory localDirectory, String file, long checksum, IndexShard indexShard) throws IOException {
        try (IndexInput indexInput = localDirectory.openInput(file, IOContext.READONCE)) {
            if (checksum == CodecUtil.retrieveChecksum(indexInput)) {
                return true;
            } else {
                indexShard.logger.warn("Checksum mismatch between local and remote segment file: {}, will override local file", file);
                // If there is a checksum mismatch and we are not serving reads it is safe to go ahead and delete the file now.
                // Outside of engine resets this method will be invoked during recovery so this is safe.
                if (indexShard.isReadAllowed() == false) {
                    localDirectory.deleteFile(file);
                } else {
                    // segment conflict with remote store while the shard is serving reads.
                    indexShard.failShard("Local copy of segment " + file + " has a different checksum than the version in remote store", null);
                }
            }
        } catch (NoSuchFileException | FileNotFoundException e) {
            indexShard.logger.debug("File {} does not exist in local FS, downloading from remote store", file);
        } catch (IOException e) {
            indexShard.logger.warn("Exception while reading checksum of file: {}, this can happen if file is corrupted", file);
            // For any other exception on reading checksum, we delete the file to re-download again
            localDirectory.deleteFile(file);
        }
        return false;
    }

    static void checkIndex(Store store, String checkIndexOnStartup, IndexShard indexShard) throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex(store, checkIndexOnStartup, indexShard);
            } catch (IOException e) {
                store.markStoreCorrupted(e);
                throw e;
            } finally {
                store.decRef();
            }
        }
    }

    private static void doCheckIndex(Store store, String checkIndexOnStartup, IndexShard indexShard) throws IOException {
        long timeNS = System.nanoTime();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }

        try (BytesStreamOutput os = new BytesStreamOutput(); PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name())) {
            if ("checksum".equals(checkIndexOnStartup)) {
                // physical verification only: verify all checksums for the latest commit
                IOException corrupt = null;
                Store.MetadataSnapshot metadata = indexShard.snapshotStoreMetadata();
                for (Map.Entry<String, StoreFileMetadata> entry : metadata.asMap().entrySet()) {
                    try {
                        Store.checkIntegrity(entry.getValue(), store.directory());
                        out.println("checksum passed: " + entry.getKey());
                    } catch (IOException exc) {
                        out.println("checksum failed: " + entry.getKey());
                        exc.printStackTrace(out);
                        corrupt = exc;
                    }
                }
                out.flush();
                if (corrupt != null) {
                    indexShard.logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                    throw corrupt;
                }
            } else {
                // full checkindex
                final CheckIndex.Status status = store.checkIndex(out);
                out.flush();
                if (!status.clean) {
                    if (indexShard.state == IndexShardState.CLOSED) {
                        // ignore if closed....
                        return;
                    }
                    indexShard.logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                    throw new IOException("index check failure");
                }
            }

            if (indexShard.logger.isDebugEnabled()) {
                indexShard.logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
            }
        }

        indexShard.recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    /**
     * Downloads segments from given remote segment store for a specific commit.
     * @param overrideLocal flag to override local segment files with those in remote store
     * @param sourceRemoteDirectory RemoteSegmentDirectory Instance from which we need to sync segments
     * @throws IOException if exception occurs while reading segments from remote store
     */
    public static void syncSegmentsFromGivenRemoteSegmentStore(
        boolean overrideLocal,
        RemoteSegmentStoreDirectory sourceRemoteDirectory,
        RemoteSegmentMetadata remoteSegmentMetadata,
        boolean pinnedTimestamp,
        IndexShard indexShard,
        Store store,
        Store remoteStore,
        RecoveryState recoveryState
    ) throws IOException {
        indexShard.logger.trace("Downloading segments from given remote segment store");
        RemoteSegmentStoreDirectory remoteDirectory = null;
        if (remoteStore != null) {
            remoteDirectory = getRemoteDirectory(remoteStore);
            remoteDirectory.init();
            remoteStore.incRef();
        }
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = sourceRemoteDirectory
            .getSegmentsUploadedToRemoteStore();
        indexShard.store.incRef();
        try {
            final Directory storeDirectory;
            if (recoveryState.getStage() == RecoveryState.Stage.INDEX) {
                storeDirectory = new StoreRecovery.StatsDirectoryWrapper(store.directory(), recoveryState.getIndex());
                for (String file : uploadedSegments.keySet()) {
                    long checksum = Long.parseLong(uploadedSegments.get(file).getChecksum());
                    if (overrideLocal || localDirectoryContains(storeDirectory, file, checksum, indexShard) == false) {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), false);
                    } else {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), true);
                    }
                }
            } else {
                storeDirectory = store.directory();
            }

            String segmentsNFile = copySegmentFiles(
                storeDirectory,
                sourceRemoteDirectory,
                remoteDirectory,
                uploadedSegments,
                overrideLocal,
                () -> {},
                indexShard
            );
            if (pinnedTimestamp) {
                final SegmentInfos infosSnapshot = store.buildSegmentInfos(
                    remoteSegmentMetadata.getSegmentInfosBytes(),
                    remoteSegmentMetadata.getGeneration()
                );
                long processedLocalCheckpoint = Long.parseLong(infosSnapshot.getUserData().get(LOCAL_CHECKPOINT_KEY));
                // delete any other commits, we want to start the engine only from a new commit made with the downloaded infos bytes.
                // Extra segments will be wiped on engine open.
                for (String file : List.of(store.directory().listAll())) {
                    if (file.startsWith(IndexFileNames.SEGMENTS)) {
                        store.deleteQuiet(file);
                    }
                }
                assert Arrays.stream(store.directory().listAll()).filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).findAny().isEmpty()
                    || indexShard.indexSettings.isWarmIndex() : "There should not be any segments file in the dir";
                store.commitSegmentInfos(infosSnapshot, processedLocalCheckpoint, processedLocalCheckpoint);
            } else if (segmentsNFile != null) {
                try (
                    ChecksumIndexInput indexInput = new BufferedChecksumIndexInput(
                        storeDirectory.openInput(segmentsNFile, IOContext.READONCE)
                    )
                ) {
                    long commitGeneration = SegmentInfos.generationFromSegmentsFileName(segmentsNFile);
                    SegmentInfos infosSnapshot = SegmentInfos.readCommit(store.directory(), indexInput, commitGeneration);
                    long processedLocalCheckpoint = Long.parseLong(infosSnapshot.getUserData().get(LOCAL_CHECKPOINT_KEY));
                    if (remoteStore != null) {
                        store.commitSegmentInfos(infosSnapshot, processedLocalCheckpoint, processedLocalCheckpoint);
                    } else {
                        store.directory().sync(infosSnapshot.files(true));
                        store.directory().syncMetaData();
                    }
                }
            }
        } catch (IOException e) {
            throw new IndexShardRecoveryException(indexShard.shardId, "Exception while copying segment files from remote segment store", e);
        } finally {
            store.decRef();
            if (remoteStore != null) {
                remoteStore.decRef();
            }
        }
    }

    /**
     * Downloads segments from remote segment store along with updating the access time of the recovery target.
     * @param overrideLocal flag to override local segment files with those in remote store.
     * @param onFileSync runnable that updates the access time when run.
     * @throws IOException if exception occurs while reading segments from remote store.
     */
    public static void syncSegmentsFromRemoteSegmentStore(boolean overrideLocal, final Runnable onFileSync, IndexShard indexShard,
                                                          Store store,
                                                          Store remoteStore,
                                                          RecoveryState recoveryState) throws IOException {
        boolean syncSegmentSuccess = false;
        long startTimeMs = System.currentTimeMillis();
        assert indexShard.indexSettings.isRemoteStoreEnabled() || indexShard.isRemoteSeeded();
        indexShard.logger.trace("Downloading segments from remote segment store");
        RemoteSegmentStoreDirectory remoteDirectory = getRemoteDirectory(remoteStore);
        // We need to call RemoteSegmentStoreDirectory.init() in order to get latest metadata of the files that
        // are uploaded to the remote segment store.
        RemoteSegmentMetadata remoteSegmentMetadata = remoteDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteDirectory
            .getSegmentsUploadedToRemoteStore()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(IndexFileNames.SEGMENTS) == false)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        store.incRef();
        remoteStore.incRef();
        try {
            final Directory storeDirectory;
            if (recoveryState.getStage() == RecoveryState.Stage.INDEX) {
                storeDirectory = new StoreRecovery.StatsDirectoryWrapper(store.directory(), recoveryState.getIndex());
                for (String file : uploadedSegments.keySet()) {
                    long checksum = Long.parseLong(uploadedSegments.get(file).getChecksum());
                    if (overrideLocal || localDirectoryContains(storeDirectory, file, checksum, indexShard) == false) {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), false);
                    } else {
                        recoveryState.getIndex().addFileDetail(file, uploadedSegments.get(file).getLength(), true);
                    }
                }
            } else {
                storeDirectory = store.directory();
            }
            if (indexShard.indexSettings.isWarmIndex() == false) {
                copySegmentFiles(storeDirectory, remoteDirectory, null, uploadedSegments, overrideLocal, onFileSync, indexShard);
            }

            if (remoteSegmentMetadata != null) {
                final SegmentInfos infosSnapshot = store.buildSegmentInfos(
                    remoteSegmentMetadata.getSegmentInfosBytes(),
                    remoteSegmentMetadata.getGeneration()
                );
                long processedLocalCheckpoint = Long.parseLong(infosSnapshot.getUserData().get(LOCAL_CHECKPOINT_KEY));
                // delete any other commits, we want to start the engine only from a new commit made with the downloaded infos bytes.
                // Extra segments will be wiped on engine open.
                for (String file : List.of(store.directory().listAll())) {
                    if (file.startsWith(IndexFileNames.SEGMENTS)) {
                        store.deleteQuiet(file);
                    }
                }
                assert Arrays.stream(store.directory().listAll()).filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).findAny().isEmpty()
                    || indexShard.indexSettings.isWarmIndex() : "There should not be any segments file in the dir";
                store.commitSegmentInfos(infosSnapshot, processedLocalCheckpoint, processedLocalCheckpoint);
            }
            syncSegmentSuccess = true;
        } catch (IOException e) {
            throw new IndexShardRecoveryException(indexShard.shardId, "Exception while copying segment files from remote segment store", e);
        } finally {
            indexShard.logger.trace(
                "syncSegmentsFromRemoteSegmentStore success={} elapsedTime={}",
                syncSegmentSuccess,
                (System.currentTimeMillis() - startTimeMs)
            );
            store.decRef();
            remoteStore.decRef();
        }
    }

    private static String copySegmentFiles(
        Directory storeDirectory,
        RemoteSegmentStoreDirectory sourceRemoteDirectory,
        RemoteSegmentStoreDirectory targetRemoteDirectory,
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments,
        boolean overrideLocal,
        final Runnable onFileSync,
        IndexShard indexShard
    ) throws IOException {
        Set<String> toDownloadSegments = new HashSet<>();
        Set<String> skippedSegments = new HashSet<>();
        String segmentNFile = null;

        try {
            if (overrideLocal) {
                for (String file : storeDirectory.listAll()) {
                    storeDirectory.deleteFile(file);
                }
            }

            for (String file : uploadedSegments.keySet()) {
                long checksum = Long.parseLong(uploadedSegments.get(file).getChecksum());
                if (overrideLocal || localDirectoryContains(storeDirectory, file, checksum, indexShard) == false) {
                    toDownloadSegments.add(file);
                } else {
                    skippedSegments.add(file);
                }

                if (file.startsWith(IndexFileNames.SEGMENTS)) {
                    assert segmentNFile == null : "There should be only one SegmentInfosSnapshot file";
                    segmentNFile = file;
                }
            }

            if (toDownloadSegments.isEmpty() == false) {
                try {
                    indexShard.getFileDownloader().download(sourceRemoteDirectory, storeDirectory, targetRemoteDirectory, toDownloadSegments, onFileSync);
                } catch (Exception e) {
                    throw new IOException("Error occurred when downloading segments from remote store", e);
                }
            }
        } finally {
            indexShard.logger.trace("Downloaded segments here: {}", toDownloadSegments);
            indexShard.logger.trace("Skipped download for segments here: {}", skippedSegments);
        }

        return segmentNFile;
    }

    /*
    ToDo : Fix this https://github.com/opensearch-project/OpenSearch/issues/8003
     */
    public static RemoteSegmentStoreDirectory getRemoteDirectory(Store remoteStore) {
        assert remoteStore.directory() instanceof FilterDirectory : "Store.directory is not an instance of FilterDirectory";
        FilterDirectory remoteStoreDirectory = (FilterDirectory) remoteStore.directory();
        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
        final Directory remoteDirectory = byteSizeCachingStoreDirectory.getDelegate();
        return ((RemoteSegmentStoreDirectory) remoteDirectory);
    }
}
