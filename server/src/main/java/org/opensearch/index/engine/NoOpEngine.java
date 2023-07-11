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

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * NoOpEngine is an engine implementation that does nothing but the bare minimum
 * required in order to have an engine. All attempts to do something (search,
 * index, get), throw {@link UnsupportedOperationException}. However, NoOpEngine
 * allows to trim any existing translog files through the usage of the
 * {{@link TranslogManager#trimUnreferencedTranslogFiles()}} method.
 *
 * @opensearch.internal
 */
public final class NoOpEngine extends ReadOnlyEngine {

    private final SegmentsStats segmentsStats;
    private final DocsStats docsStats;

    public NoOpEngine(EngineConfig config) {
        super(config, null, null, true, Function.identity(), true);
        this.segmentsStats = new SegmentsStats();
        Directory directory = store.directory();
        try (DirectoryReader reader = openDirectory(directory, config.getIndexSettings().isSoftDeleteEnabled())) {
            for (LeafReaderContext ctx : reader.getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                fillSegmentStats(segmentReader, true, segmentsStats);
            }
            this.docsStats = docsStats(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected DirectoryReader open(final IndexCommit commit) throws IOException {
        final Directory directory = commit.getDirectory();
        final List<IndexCommit> indexCommits = DirectoryReader.listCommits(directory);
        final IndexCommit indexCommit = indexCommits.get(indexCommits.size() - 1);
        return new DirectoryReader(directory, new LeafReader[0], null) {
            @Override
            protected DirectoryReader doOpenIfChanged() {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
                return null;
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isCurrent() {
                return true;
            }

            @Override
            public IndexCommit getIndexCommit() {
                return indexCommit;
            }

            @Override
            protected void doClose() {}

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        if (includeUnloadedSegments) {
            final SegmentsStats stats = new SegmentsStats();
            stats.add(this.segmentsStats);
            if (includeSegmentFileSizes == false) {
                stats.clearFileSizes();
            }
            return stats;
        } else {
            return super.segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
        }
    }

    @Override
    public DocsStats docStats() {
        return docsStats;
    }

    /**
     * This implementation will trim existing translog files using a {@link TranslogDeletionPolicy}
     * that retains nothing but the last translog generation from safe commit.
     */
    public TranslogManager translogManager() {
        try {
            return new NoOpTranslogManager(shardId, readLock, this::ensureOpen, this.translogStats, new Translog.Snapshot() {
                @Override
                public void close() {}

                @Override
                public int totalOperations() {
                    return 0;
                }

                @Override
                public Translog.Operation next() {
                    return null;
                }

            }) {
                /**
                 * This implementation will trim existing translog files using a {@link TranslogDeletionPolicy}
                 * that retains nothing but the last translog generation from safe commit.
                 */
                @Override
                public void trimUnreferencedTranslogFiles() throws TranslogException {
                    final Store store = engineConfig.getStore();
                    store.incRef();
                    try (ReleasableLock ignored = readLock.acquire()) {
                        ensureOpen();
                        final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
                        if (commits.size() == 1 && translogStats.getTranslogSizeInBytes() > translogStats.getUncommittedSizeInBytes()) {
                            final Map<String, String> commitUserData = getLastCommittedSegmentInfos().getUserData();
                            final String translogUuid = commitUserData.get(Translog.TRANSLOG_UUID_KEY);
                            if (translogUuid == null) {
                                throw new IllegalStateException("commit doesn't contain translog unique id");
                            }
                            final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
                            final long localCheckpoint = Long.parseLong(commitUserData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
                            final TranslogDeletionPolicy translogDeletionPolicy = new DefaultTranslogDeletionPolicy(-1, -1, 0);
                            translogDeletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
                            try (
                                Translog translog = engineConfig.getTranslogFactory()
                                    .newTranslog(
                                        translogConfig,
                                        translogUuid,
                                        translogDeletionPolicy,
                                        engineConfig.getGlobalCheckpointSupplier(),
                                        engineConfig.getPrimaryTermSupplier(),
                                        seqNo -> {},
                                        engineConfig.getPrimaryModeSupplier()
                                    )
                            ) {
                                translog.trimUnreferencedReaders();
                                // refresh the translog stats
                                translogStats = translog.stats();
                                // When remote translog is enabled, the min file generation is dependent on the (N-1)
                                // lastRefreshedCheckpoint SeqNo - refer RemoteStoreRefreshListener. This leads to older generations not
                                // being trimmed and leading to current generation being higher than the min file generation.
                                assert engineConfig.getIndexSettings().isRemoteTranslogStoreEnabled()
                                    || translog.currentFileGeneration() == translog.getMinFileGeneration() : "translog was not trimmed "
                                        + " current gen "
                                        + translog.currentFileGeneration()
                                        + " != min gen "
                                        + translog.getMinFileGeneration();
                            }
                        }
                    } catch (final Exception e) {
                        try {
                            failEngine("translog trimming failed", e);
                        } catch (Exception inner) {
                            e.addSuppressed(inner);
                        }
                        throw new EngineException(shardId, "failed to trim translog", e);
                    } finally {
                        store.decRef();
                    }
                }

                @Override
                public void setMinSeqNoToKeep(long seqNo) {}
            };
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
