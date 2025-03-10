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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.snapshots.IndexShardRestoreFailedException;
import org.opensearch.index.snapshots.blobstore.RemoteStoreShardShallowCopySnapshot;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.Checkpoint;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogHeader;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.index.translog.Translog.CHECKPOINT_FILE_NAME;

/**
 * This package private utility class encapsulates the logic to recover an index shard from either an existing index on
 * disk or from a snapshot in a repository.
 *
 * @opensearch.internal
 */
final class StoreRecovery {

    private final Logger logger;
    private final ShardId shardId;

    StoreRecovery(ShardId shardId, Logger logger) {
        this.logger = logger;
        this.shardId = shardId;
    }

    /**
     * Recovers a shard from it's local file system store. This method required pre-knowledge about if the shard should
     * exist on disk ie. has been previously allocated or if the shard is a brand new allocation without pre-existing index
     * files / transaction logs. This
     * @param indexShard the index shard instance to recovery the shard into
     * @param listener resolves to <code>true</code> if the shard has been recovered successfully, <code>false</code> if the recovery
     *                 has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     * @see Store
     */
    void recoverFromStore(final IndexShard indexShard, ActionListener<Boolean> listener) {
        if (canRecover(indexShard)) {
            ActionListener.completeWith(recoveryListener(indexShard, listener), () -> {
                logger.debug("starting recovery from store ...");
                internalRecoverFromStore(indexShard);
                return true;
            });
        } else {
            listener.onResponse(false);
        }
    }

    void recoverFromRemoteStore(final IndexShard indexShard, ActionListener<Boolean> listener) {
        if (canRecover(indexShard)) {
            RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
            assert recoveryType == RecoverySource.Type.REMOTE_STORE : "expected remote store recovery type but was: " + recoveryType;
            ActionListener.completeWith(recoveryListener(indexShard, listener), () -> {
                logger.debug("starting recovery from remote store ...");
                recoverFromRemoteStore(indexShard);
                return true;
            });
        } else {
            listener.onResponse(false);
        }
    }

    void recoverFromLocalShards(
        Consumer<MappingMetadata> mappingUpdateConsumer,
        IndexShard indexShard,
        final List<LocalShardSnapshot> shards,
        ActionListener<Boolean> listener
    ) {
        if (canRecover(indexShard)) {
            RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
            assert recoveryType == RecoverySource.Type.LOCAL_SHARDS : "expected local shards recovery type: " + recoveryType;
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("shards must not be empty");
            }
            Set<Index> indices = shards.stream().map((s) -> s.getIndex()).collect(Collectors.toSet());
            if (indices.size() > 1) {
                throw new IllegalArgumentException("can't add shards from more than one index");
            }
            IndexMetadata sourceMetadata = shards.get(0).getIndexMetadata();
            if (sourceMetadata.mapping() != null) {
                mappingUpdateConsumer.accept(sourceMetadata.mapping());
            }
            indexShard.mapperService().merge(sourceMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
            // now that the mapping is merged we can validate the index sort configuration.
            Sort indexSort = indexShard.getIndexSort();
            final boolean hasNested = indexShard.mapperService().hasNested();
            final boolean isSplit = sourceMetadata.getNumberOfShards() < indexShard.indexSettings().getNumberOfShards();
            ActionListener.completeWith(recoveryListener(indexShard, listener), () -> {
                logger.debug("starting recovery from local shards {}", shards);
                try {
                    final Directory directory = indexShard.store().directory(); // don't close this directory!!
                    final Directory[] sources = shards.stream().map(LocalShardSnapshot::getSnapshotDirectory).toArray(Directory[]::new);
                    final long maxSeqNo = shards.stream().mapToLong(LocalShardSnapshot::maxSeqNo).max().getAsLong();
                    final long maxUnsafeAutoIdTimestamp = shards.stream()
                        .mapToLong(LocalShardSnapshot::maxUnsafeAutoIdTimestamp)
                        .max()
                        .getAsLong();
                    addIndices(
                        indexShard.recoveryState().getIndex(),
                        directory,
                        indexSort,
                        sources,
                        maxSeqNo,
                        maxUnsafeAutoIdTimestamp,
                        indexShard.indexSettings().getIndexMetadata(),
                        indexShard.shardId().id(),
                        isSplit,
                        hasNested
                    );
                    internalRecoverFromStore(indexShard);
                    // just trigger a merge to do housekeeping on the
                    // copied segments - we will also see them in stats etc.
                    indexShard.getEngine().forceMerge(false, -1, false, false, false, UUIDs.randomBase64UUID());
                    if (indexShard.isRemoteTranslogEnabled() && indexShard.shardRouting.primary()) {
                        indexShard.waitForRemoteStoreSync();
                    }
                    return true;
                } catch (IOException ex) {
                    throw new IndexShardRecoveryException(indexShard.shardId(), "failed to recover from local shards", ex);
                }
            });
        } else {
            listener.onResponse(false);
        }
    }

    void addIndices(
        final ReplicationLuceneIndex indexRecoveryStats,
        final Directory target,
        final Sort indexSort,
        final Directory[] sources,
        final long maxSeqNo,
        final long maxUnsafeAutoIdTimestamp,
        IndexMetadata indexMetadata,
        int shardId,
        boolean split,
        boolean hasNested
    ) throws IOException {

        assert sources.length > 0;
        final int luceneIndexCreatedVersionMajor = Lucene.readSegmentInfos(sources[0]).getIndexCreatedVersionMajor();

        final Directory hardLinkOrCopyTarget = new org.apache.lucene.misc.store.HardlinkCopyDirectoryWrapper(target);

        IndexWriterConfig iwc = new IndexWriterConfig(null).setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setCommitOnClose(false)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setIndexCreatedVersionMajor(luceneIndexCreatedVersionMajor);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }

        try (IndexWriter writer = new IndexWriter(new StatsDirectoryWrapper(hardLinkOrCopyTarget, indexRecoveryStats), iwc)) {
            writer.addIndexes(sources);
            indexRecoveryStats.setFileDetailsComplete();
            if (split) {
                writer.deleteDocuments(new ShardSplittingQuery(indexMetadata, shardId, hasNested));
            }
            /*
             * We set the maximum sequence number and the local checkpoint on the target to the maximum of the maximum sequence numbers on
             * the source shards. This ensures that history after this maximum sequence number can advance and we have correct
             * document-level semantics.
             */
            writer.setLiveCommitData(() -> {
                final HashMap<String, String> liveCommitData = new HashMap<>(3);
                liveCommitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
                liveCommitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
                liveCommitData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp));
                return liveCommitData.entrySet().iterator();
            });
            writer.commit();
        }
    }

    /**
     * Directory wrapper that records copy process for recovery statistics
     *
     * @opensearch.internal
     */
    static final class StatsDirectoryWrapper extends FilterDirectory {
        private final ReplicationLuceneIndex index;

        StatsDirectoryWrapper(Directory in, ReplicationLuceneIndex indexRecoveryStats) {
            super(in);
            this.index = indexRecoveryStats;
        }

        @Override
        public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
            final long l = from.fileLength(src);
            final AtomicBoolean copies = new AtomicBoolean(false);
            // here we wrap the index input form the source directory to report progress of file copy for the recovery stats.
            // we increment the num bytes recovered in the readBytes method below, if users pull statistics they can see immediately
            // how much has been recovered.
            in.copyFrom(new FilterDirectory(from) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    if (index.getFileDetails(dest) == null) {
                        index.addFileDetail(dest, l, false);
                    }
                    copies.set(true);
                    final IndexInput input = in.openInput(name, context);
                    return new IndexInput("StatsDirectoryWrapper(" + input.toString() + ")") {
                        @Override
                        public void close() throws IOException {
                            input.close();
                        }

                        @Override
                        public long getFilePointer() {
                            throw new UnsupportedOperationException("only straight copies are supported");
                        }

                        @Override
                        public void seek(long pos) throws IOException {
                            throw new UnsupportedOperationException("seeks are not supported");
                        }

                        @Override
                        public long length() {
                            return input.length();
                        }

                        @Override
                        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                            throw new UnsupportedOperationException("slices are not supported");
                        }

                        @Override
                        public byte readByte() throws IOException {
                            throw new UnsupportedOperationException("use a buffer if you wanna perform well");
                        }

                        @Override
                        public void readBytes(byte[] b, int offset, int len) throws IOException {
                            // we rely on the fact that copyFrom uses a buffer
                            input.readBytes(b, offset, len);
                            index.addRecoveredBytesToFile(dest, len);
                        }
                    };
                }
            }, src, dest, context);
            if (copies.get() == false && index.getFileDetails(dest) == null) {
                index.addFileDetail(dest, l, true); // hardlinked - we treat it as reused since the file was already somewhat there
            } else {
                assert index.getFileDetails(dest) != null : "File [" + dest + "] has no file details";
                assert index.getFileDetails(dest).recovered() == l : index.getFileDetails(dest).toString();
            }
        }

        // temporary override until LUCENE-8735 is integrated
        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return in.getPendingDeletions();
        }
    }

    /**
     * Recovers an index from a given {@link Repository}. This method restores a
     * previously created index snapshot into an existing initializing shard.
     * @param indexShard the index shard instance to recovery the snapshot from
     * @param repository the repository holding the physical files the shard should be recovered from
     * @param listener resolves to <code>true</code> if the shard has been recovered successfully, <code>false</code> if the recovery
     *                 has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     */
    void recoverFromRepository(final IndexShard indexShard, Repository repository, ActionListener<Boolean> listener) {
        try {
            if (canRecover(indexShard)) {
                RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
                assert recoveryType == RecoverySource.Type.SNAPSHOT : "expected snapshot recovery type: " + recoveryType;
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource();
                restore(indexShard, repository, recoverySource, recoveryListener(indexShard, listener));
            } else {
                listener.onResponse(false);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    void recoverFromSnapshotAndRemoteStore(
        final IndexShard indexShard,
        Repository repository,
        RepositoriesService repositoriesService,
        ActionListener<Boolean> listener,
        String segmentsPathFixedPrefix,
        ThreadPool threadPool
    ) {
        try {
            if (canRecover(indexShard)) {
                indexShard.preRecovery();
                RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
                assert recoveryType == RecoverySource.Type.SNAPSHOT : "expected snapshot recovery type: " + recoveryType;
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource();
                final RecoveryState.Translog translogState = indexShard.recoveryState().getTranslog();
                translogState.totalOperations(0);
                translogState.totalOperationsOnStart(0);
                indexShard.prepareForIndexRecovery();

                RemoteStoreShardShallowCopySnapshot shallowCopyShardMetadata = repository.getRemoteStoreShallowCopyShardMetadata(
                    recoverySource.snapshot().getSnapshotId(),
                    recoverySource.index(),
                    shardId
                );

                long primaryTerm = shallowCopyShardMetadata.getPrimaryTerm();
                long commitGeneration = shallowCopyShardMetadata.getCommitGeneration();
                String indexUUID = shallowCopyShardMetadata.getIndexUUID();
                String remoteStoreRepository = ((SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource())
                    .sourceRemoteStoreRepository();
                if (remoteStoreRepository == null) {
                    remoteStoreRepository = shallowCopyShardMetadata.getRemoteStoreRepository();
                }

                RemoteSegmentStoreDirectoryFactory directoryFactory = new RemoteSegmentStoreDirectoryFactory(
                    () -> repositoriesService,
                    threadPool,
                    segmentsPathFixedPrefix
                );
                RemoteSegmentStoreDirectory sourceRemoteDirectory = (RemoteSegmentStoreDirectory) directoryFactory.newDirectory(
                    remoteStoreRepository,
                    indexUUID,
                    shardId,
                    shallowCopyShardMetadata.getRemoteStorePathStrategy()
                );
                RemoteSegmentMetadata remoteSegmentMetadata = sourceRemoteDirectory.initializeToSpecificCommit(
                    primaryTerm,
                    commitGeneration,
                    recoverySource.snapshot().getSnapshotId().getUUID()
                );
                indexShard.syncSegmentsFromGivenRemoteSegmentStore(true, sourceRemoteDirectory, remoteSegmentMetadata, false);
                final Store store = indexShard.store();
                if (indexShard.indexSettings.isRemoteStoreEnabled() == false) {
                    bootstrap(indexShard, store);
                } else {
                    bootstrapForSnapshot(indexShard, store);
                }
                assert indexShard.shardRouting.primary() : "only primary shards can recover from store";
                writeEmptyRetentionLeasesFile(indexShard);
                indexShard.recoveryState().getIndex().setFileDetailsComplete();
                if (indexShard.indexSettings.isRemoteStoreEnabled()) {
                    indexShard.openEngineAndSkipTranslogRecoveryFromSnapshot();
                } else {
                    indexShard.openEngineAndRecoverFromTranslog();
                }
                indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
                indexShard.finalizeRecovery();
                if (indexShard.isRemoteTranslogEnabled() && indexShard.shardRouting.primary()) {
                    indexShard.waitForRemoteStoreSync();
                }
                indexShard.postRecovery("restore done");

                listener.onResponse(true);
            } else {
                listener.onResponse(false);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    void recoverShallowSnapshotV2(
        final IndexShard indexShard,
        Repository repository,
        RepositoriesService repositoriesService,
        ActionListener<Boolean> listener,
        String segmentsPathFixedPrefix,
        ThreadPool threadPool
    ) {
        try {
            if (canRecover(indexShard)) {
                indexShard.preRecovery();
                RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
                assert recoveryType == RecoverySource.Type.SNAPSHOT : "expected snapshot recovery type: " + recoveryType;
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource();
                indexShard.prepareForIndexRecovery();

                assert recoverySource.pinnedTimestamp() != 0;
                final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
                repository.getRepositoryData(repositoryDataListener);
                repositoryDataListener.whenComplete(repositoryData -> {
                    IndexId indexId = repositoryData.resolveIndexId(recoverySource.index().getName());
                    IndexMetadata prevIndexMetadata = repository.getSnapshotIndexMetaData(
                        repositoryData,
                        recoverySource.snapshot().getSnapshotId(),
                        indexId
                    );
                    RemoteSegmentStoreDirectoryFactory directoryFactory = new RemoteSegmentStoreDirectoryFactory(
                        () -> repositoriesService,
                        threadPool,
                        segmentsPathFixedPrefix
                    );
                    String remoteSegmentStoreRepository = ((SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource())
                        .sourceRemoteStoreRepository();
                    if (remoteSegmentStoreRepository == null) {
                        remoteSegmentStoreRepository = IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.get(
                            prevIndexMetadata.getSettings()
                        );
                    }
                    RemoteStorePathStrategy remoteStorePathStrategy = RemoteStoreUtils.determineRemoteStorePathStrategy(prevIndexMetadata);
                    RemoteSegmentStoreDirectory sourceRemoteDirectory = (RemoteSegmentStoreDirectory) directoryFactory.newDirectory(
                        remoteSegmentStoreRepository,
                        prevIndexMetadata.getIndexUUID(),
                        shardId,
                        remoteStorePathStrategy
                    );
                    RemoteSegmentMetadata remoteSegmentMetadata = sourceRemoteDirectory.initializeToSpecificTimestamp(
                        recoverySource.pinnedTimestamp()
                    );

                    String remoteTranslogRepository = ((SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource())
                        .sourceRemoteTranslogRepository();
                    if (remoteTranslogRepository == null) {
                        remoteTranslogRepository = IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.get(
                            prevIndexMetadata.getSettings()
                        );
                    }

                    indexShard.syncSegmentsFromGivenRemoteSegmentStore(true, sourceRemoteDirectory, remoteSegmentMetadata, true);
                    indexShard.syncTranslogFilesFromGivenRemoteTranslog(
                        repositoriesService.repository(remoteTranslogRepository),
                        new ShardId(prevIndexMetadata.getIndex(), shardId.id()),
                        remoteStorePathStrategy,
                        RemoteStoreUtils.determineTranslogMetadataEnabled(prevIndexMetadata),
                        recoverySource.pinnedTimestamp()
                    );

                    assert indexShard.shardRouting.primary() : "only primary shards can recover from store";
                    writeEmptyRetentionLeasesFile(indexShard);
                    indexShard.recoveryState().getIndex().setFileDetailsComplete();
                    indexShard.openEngineAndRecoverFromTranslog(false);
                    indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
                    indexShard.finalizeRecovery();
                    if (indexShard.isRemoteTranslogEnabled() && indexShard.shardRouting.primary()) {
                        indexShard.waitForRemoteStoreSync();
                    }
                    indexShard.postRecovery("post recovery from remote_store");
                    SegmentInfos committedSegmentInfos = indexShard.store().readLastCommittedSegmentsInfo();
                    try {
                        indexShard.getEngine()
                            .translogManager()
                            .setMinSeqNoToKeep(Long.parseLong(committedSegmentInfos.getUserData().get(SequenceNumbers.MAX_SEQ_NO)) + 1);
                    } catch (IllegalArgumentException e) {
                        logger.warn("MinSeqNoToKeep is already past the maxSeqNo from commited segment infos");
                    }
                    listener.onResponse(true);
                }, listener::onFailure);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private boolean canRecover(IndexShard indexShard) {
        if (indexShard.state() == IndexShardState.CLOSED) {
            // got closed on us, just ignore this recovery
            return false;
        }
        if (indexShard.routingEntry().primary() == false && indexShard.routingEntry().isSearchOnly() == false) {
            throw new IndexShardRecoveryException(shardId, "Trying to recover when the shard is in backup state", null);
        }
        return true;
    }

    private ActionListener<Boolean> recoveryListener(IndexShard indexShard, ActionListener<Boolean> listener) {
        return ActionListener.wrap(res -> {
            if (res) {
                // Check that the gateway didn't leave the shard in init or recovering stage. it is up to the gateway
                // to call post recovery.
                final IndexShardState shardState = indexShard.state();
                final RecoveryState recoveryState = indexShard.recoveryState();
                assert shardState != IndexShardState.CREATED && shardState != IndexShardState.RECOVERING : "recovery process of "
                    + shardId
                    + " didn't get to post_recovery. shardState ["
                    + shardState
                    + "]";

                if (logger.isTraceEnabled()) {
                    ReplicationLuceneIndex index = recoveryState.getIndex();
                    StringBuilder sb = new StringBuilder();
                    sb.append("    index    : files           [")
                        .append(index.totalFileCount())
                        .append("] with total_size [")
                        .append(new ByteSizeValue(index.totalBytes()))
                        .append("], took[")
                        .append(TimeValue.timeValueMillis(index.time()))
                        .append("]\n");
                    sb.append("             : recovered_files [")
                        .append(index.recoveredFileCount())
                        .append("] with total_size [")
                        .append(new ByteSizeValue(index.recoveredBytes()))
                        .append("]\n");
                    sb.append("             : reusing_files   [")
                        .append(index.reusedFileCount())
                        .append("] with total_size [")
                        .append(new ByteSizeValue(index.reusedBytes()))
                        .append("]\n");
                    sb.append("    verify_index    : took [")
                        .append(TimeValue.timeValueMillis(recoveryState.getVerifyIndex().time()))
                        .append("], check_index [")
                        .append(timeValueMillis(recoveryState.getVerifyIndex().checkIndexTime()))
                        .append("]\n");
                    sb.append("    translog : number_of_operations [")
                        .append(recoveryState.getTranslog().recoveredOperations())
                        .append("], took [")
                        .append(TimeValue.timeValueMillis(recoveryState.getTranslog().time()))
                        .append("]");
                    logger.trace(
                        "recovery completed from [shard_store], took [{}]\n{}",
                        timeValueMillis(recoveryState.getTimer().time()),
                        sb
                    );
                } else if (logger.isDebugEnabled()) {
                    logger.debug("recovery completed from [shard_store], took [{}]", timeValueMillis(recoveryState.getTimer().time()));
                }
            }
            listener.onResponse(res);
        }, ex -> {
            if (ex instanceof IndexShardRecoveryException) {
                if (indexShard.state() == IndexShardState.CLOSED) {
                    // got closed on us, just ignore this recovery
                    listener.onResponse(false);
                    return;
                }
                if ((ex.getCause() instanceof IndexShardClosedException) || (ex.getCause() instanceof IndexShardNotStartedException)) {
                    // got closed on us, just ignore this recovery
                    listener.onResponse(false);
                    return;
                }
                listener.onFailure(ex);
            } else if (ex instanceof IndexShardClosedException || ex instanceof IndexShardNotStartedException) {
                listener.onResponse(false);
            } else {
                if (indexShard.state() == IndexShardState.CLOSED) {
                    // got closed on us, just ignore this recovery
                    listener.onResponse(false);
                } else {
                    listener.onFailure(new IndexShardRecoveryException(shardId, "failed recovery", ex));
                }
            }
        });
    }

    private void recoverFromRemoteStore(IndexShard indexShard) throws IndexShardRecoveryException {
        final Store remoteStore = indexShard.remoteStore();
        if (remoteStore == null) {
            throw new IndexShardRecoveryException(
                indexShard.shardId(),
                "Remote store is not enabled for this index",
                new IllegalArgumentException()
            );
        }
        indexShard.preRecovery();
        indexShard.prepareForIndexRecovery();
        final Store store = indexShard.store();
        store.incRef();
        remoteStore.incRef();
        try {
            // Download segments from remote segment store
            indexShard.syncSegmentsFromRemoteSegmentStore(true);
            indexShard.syncTranslogFilesFromRemoteTranslog();

            // On index creation, the only segment file that is created is segments_N. We can safely discard this file
            // as there is no data associated with this shard as part of segments.
            if (store.directory().listAll().length <= 1) {
                Path location = indexShard.shardPath().resolveTranslog();
                Checkpoint checkpoint = Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME));
                final Path translogFile = location.resolve(Translog.getFilename(checkpoint.getGeneration()));
                try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                    TranslogHeader translogHeader = TranslogHeader.read(translogFile, channel);
                    store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion, translogHeader.getTranslogUUID());
                }
            }

            assert indexShard.shardRouting.primary() : "only primary shards can recover from store";
            indexShard.recoveryState().getIndex().setFileDetailsComplete();
            indexShard.openEngineAndRecoverFromTranslog();
            indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
            indexShard.finalizeRecovery();
            indexShard.postRecovery("post recovery from remote_store");
        } catch (IOException | IndexShardRecoveryException e) {
            throw new IndexShardRecoveryException(indexShard.shardId, "Exception while recovering from remote store", e);
        } finally {
            store.decRef();
            remoteStore.decRef();
        }
    }

    /**
     * Recovers the state of the shard from the store.
     */
    private void internalRecoverFromStore(IndexShard indexShard) throws IndexShardRecoveryException {
        indexShard.preRecovery();
        final RecoveryState recoveryState = indexShard.recoveryState();
        final boolean indexShouldExists = recoveryState.getRecoverySource().getType() != RecoverySource.Type.EMPTY_STORE;
        indexShard.prepareForIndexRecovery();
        SegmentInfos si = null;
        final Store store = indexShard.store();
        store.incRef();
        try {
            try {
                store.failIfCorrupted();
                try {
                    si = store.readLastCommittedSegmentsInfo();
                } catch (Exception e) {
                    String files = "_unknown_";
                    try {
                        files = Arrays.toString(store.directory().listAll());
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        files += " (failure=" + ExceptionsHelper.detailedMessage(inner) + ")";
                    }
                    if (indexShouldExists) {
                        // If RecoverySource.Type is EXISTING_STORE but SegmentsInfo (si) is null,
                        // it indicates a node-left scenario where the search shard was unassigned
                        // and is now recovering on a new node that lacks SegmentsInfo.
                        // In this case, fallback to empty store recovery.
                        if (!indexShard.shardRouting.isSearchOnly()) {
                            throw new IndexShardRecoveryException(
                                shardId,
                                "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files,
                                e
                            );
                        }
                    }
                }
                // We should cover this scenario otherwise unwanted files will be in the index
                if (si != null && indexShouldExists == false) {
                    // it exists on the directory, but shouldn't exist on the FS, its a leftover (possibly dangling)
                    // its a "new index create" API, we have to do something, so better to clean it than use same data
                    Lucene.cleanLuceneIndex(store.directory());
                    si = null;
                }
            } catch (Exception e) {
                throw new IndexShardRecoveryException(shardId, "failed to fetch index version after copying it over", e);
            }
            if (recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
                assert indexShouldExists;
                bootstrap(indexShard, store);
                writeEmptyRetentionLeasesFile(indexShard);
            } else if (indexShouldExists) {
                if (si != null) {
                    if (recoveryState.getRecoverySource().shouldBootstrapNewHistoryUUID()) {
                        store.bootstrapNewHistory();
                        writeEmptyRetentionLeasesFile(indexShard);
                    }
                    // since we recover from local, just fill the files and size
                    recoverLocalFiles(recoveryState, si, store);
                } else {
                    // fallback to empty store recovery if local SegmentsInfo is null
                    recoverEmptyStore(indexShard, store);
                }
            } else {
                // shard recovery source is EMPTY_STORE
                recoverEmptyStore(indexShard, store);
            }
            if (indexShard.routingEntry().isSearchOnly() == false) {
                indexShard.openEngineAndRecoverFromTranslog();
            } else {
                // Opens the engine for pull based replica copies that are
                // not primary eligible. This will skip any checkpoint tracking and ensure
                // that the shards are sync'd with remote store before opening.
                //
                // first bootstrap new history / translog so that the TranslogUUID matches the UUID from the latest commit.
                bootstrapForSnapshot(indexShard, store);
                indexShard.openEngineAndSkipTranslogRecoveryFromSnapshot();
            }
            if (indexShard.shouldSeedRemoteStore()) {
                indexShard.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                    logger.info("Attempting to seed Remote Store via local recovery for {}", indexShard.shardId());
                    indexShard.refresh("remote store migration");
                });
                indexShard.waitForRemoteStoreSync();
                logger.info("Remote Store is now seeded via local recovery for {}", indexShard.shardId());
            }
            indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
            indexShard.finalizeRecovery();
            indexShard.postRecovery("post recovery from shard_store");
        } catch (EngineException | IOException e) {
            throw new IndexShardRecoveryException(shardId, "failed to recover from gateway", e);
        } finally {
            store.decRef();
        }
    }

    private void recoverEmptyStore(IndexShard indexShard, Store store) throws IOException {
        store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
        final String translogUUID = Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            indexShard.getPendingPrimaryTerm()
        );
        store.associateIndexWithNewTranslog(translogUUID);
        writeEmptyRetentionLeasesFile(indexShard);
        indexShard.recoveryState().getIndex().setFileDetailsComplete();
    }

    private void recoverLocalFiles(RecoveryState recoveryState, SegmentInfos si, Store store) {
        final ReplicationLuceneIndex index = recoveryState.getIndex();
        try {
            if (si != null) {
                addRecoveredFileDetails(si, store, index);
            }
        } catch (IOException e) {
            logger.debug("failed to list file details", e);
        }
        index.setFileDetailsComplete();
    }

    private static void writeEmptyRetentionLeasesFile(IndexShard indexShard) throws IOException {
        assert indexShard.getRetentionLeases().leases().isEmpty() : indexShard.getRetentionLeases(); // not loaded yet
        indexShard.persistRetentionLeases();
        assert indexShard.loadRetentionLeases().leases().isEmpty();
    }

    private void addRecoveredFileDetails(SegmentInfos si, Store store, ReplicationLuceneIndex index) throws IOException {
        final Directory directory = store.directory();
        for (String name : Lucene.files(si)) {
            long length = directory.fileLength(name);
            index.addFileDetail(name, length, true);
        }
    }

    /**
     * Restores shard from {@link SnapshotRecoverySource} associated with this shard in routing table
     */
    private void restore(
        IndexShard indexShard,
        Repository repository,
        SnapshotRecoverySource restoreSource,
        ActionListener<Boolean> listener
    ) {
        logger.debug("restoring from {} ...", indexShard.recoveryState().getRecoverySource());
        indexShard.preRecovery();
        final RecoveryState.Translog translogState = indexShard.recoveryState().getTranslog();
        if (restoreSource == null) {
            listener.onFailure(new IndexShardRestoreFailedException(shardId, "empty restore source"));
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] restoring shard [{}]", restoreSource.snapshot(), shardId);
        }
        final ActionListener<Void> restoreListener = ActionListener.wrap(v -> {
            final Store store = indexShard.store();
            if (indexShard.indexSettings.isRemoteTranslogStoreEnabled() == false) {
                bootstrap(indexShard, store);
            } else {
                bootstrapForSnapshot(indexShard, store);
            }
            assert indexShard.shardRouting.primary() : "only primary shards can recover from store";
            writeEmptyRetentionLeasesFile(indexShard);
            if (indexShard.indexSettings.isRemoteStoreEnabled()) {
                indexShard.openEngineAndSkipTranslogRecoveryFromSnapshot();
            } else {
                indexShard.openEngineAndRecoverFromTranslog();
            }
            indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
            indexShard.finalizeRecovery();
            if (indexShard.isRemoteTranslogEnabled() && indexShard.shardRouting.primary()) {
                indexShard.waitForRemoteStoreSync();
            }
            indexShard.postRecovery("restore done");
            listener.onResponse(true);
        }, e -> listener.onFailure(new IndexShardRestoreFailedException(shardId, "restore failed", e)));
        try {
            translogState.totalOperations(0);
            translogState.totalOperationsOnStart(0);
            indexShard.prepareForIndexRecovery();
            final ShardId snapshotShardId;
            final IndexId indexId = restoreSource.index();
            if (shardId.getIndexName().equals(indexId.getName())) {
                snapshotShardId = shardId;
            } else {
                snapshotShardId = new ShardId(indexId.getName(), IndexMetadata.INDEX_UUID_NA_VALUE, shardId.id());
            }
            final StepListener<IndexId> indexIdListener = new StepListener<>();
            // If the index UUID was not found in the recovery source we will have to load RepositoryData and resolve it by index name
            if (indexId.getId().equals(IndexMetadata.INDEX_UUID_NA_VALUE)) {
                // BwC path, running against an old version cluster-manager that did not add the IndexId to the recovery source
                repository.getRepositoryData(
                    ActionListener.map(indexIdListener, repositoryData -> repositoryData.resolveIndexId(indexId.getName()))
                );
            } else {
                indexIdListener.onResponse(indexId);
            }
            assert indexShard.getEngineOrNull() == null;
            indexIdListener.whenComplete(
                idx -> repository.restoreShard(
                    indexShard.store(),
                    restoreSource.snapshot().getSnapshotId(),
                    idx,
                    snapshotShardId,
                    indexShard.recoveryState(),
                    restoreListener
                ),
                restoreListener::onFailure
            );
        } catch (Exception e) {
            restoreListener.onFailure(e);
        }
    }

    private void bootstrapForSnapshot(final IndexShard indexShard, final Store store) throws IOException {
        store.bootstrapNewHistory();
        final SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
        final long localCheckpoint = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
        Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(),
            shardId,
            localCheckpoint,
            indexShard.getPendingPrimaryTerm(),
            translogUUID,
            FileChannel::open
        );
    }

    private void bootstrap(final IndexShard indexShard, final Store store) throws IOException {
        store.bootstrapNewHistory();
        final SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
        final long localCheckpoint = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));

        final String translogUUID = Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(),
            localCheckpoint,
            shardId,
            indexShard.getPendingPrimaryTerm()
        );
        store.associateIndexWithNewTranslog(translogUUID);
    }
}
