/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.manage;


import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.exec.bridge.CommitData;
import org.opensearch.index.engine.exec.bridge.ConfigurationProvider;
import org.opensearch.index.engine.exec.bridge.OperationMapper;
import org.opensearch.index.engine.exec.engine.IndexingConfiguration;
import org.opensearch.index.engine.exec.engine.RefreshInput;
import org.opensearch.index.engine.exec.engine.WriteResult;
import org.opensearch.index.engine.exec.bridge.Indexer;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.format.DataSourceRegistry;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Composable engine implementation.
 */
public class ComposableEngine implements Indexer {  //Internal Engine

    private final CompositeIndexingExecutionEngine engine;
    private List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private CatalogSnapshot catalogSnapshot;

    /**
     * Creates a new composable engine.
     * @param dataSourceRegistry the data source registry
     * @param indexingConfiguration the indexing configuration
     */
    public ComposableEngine(DataSourceRegistry dataSourceRegistry, IndexingConfiguration indexingConfiguration) {
        this.engine = new CompositeIndexingExecutionEngine(dataSourceRegistry, indexingConfiguration);
    }

    /**
     * Creates a document input.
     * @return the document input
     * @throws IOException if an I/O error occurs
     */
    public CompositeDataFormatWriter.CompositeDocumentInput documentInput() throws IOException {
        return engine.createWriter().newDocumentInput();
    }

    /**
     * Indexes a document.
     * @param index the index operation
     * @return the index result
     * @throws IOException if an I/O error occurs
     */
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        WriteResult writeResult = index.getDocumentInput().addToWriter();
        // TODO: translog, checkpoint, other checks
        return new Engine.IndexResult(writeResult.version(), writeResult.seqNo(), writeResult.term(), writeResult.success());
    }

    /**
     * Refreshes the engine.
     * @param source the refresh source
     * @throws EngineException if an engine error occurs
     */
    public synchronized void refresh(String source) throws EngineException {
        refreshListeners.forEach(ref -> {
            try {
                ref.beforeRefresh();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });


        long id = 0L;
        if (catalogSnapshot != null) {
            id = catalogSnapshot.getId();
        }
        CatalogSnapshot newCatSnap = new CatalogSnapshot(engine.refresh(new RefreshInput()), id + 1L);
        newCatSnap.incRef();
        if (catalogSnapshot != null) {
            catalogSnapshot.decRef();
        }
        catalogSnapshot = newCatSnap;

        refreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }


    /**
     * Acquires a snapshot of the catalog.
     * @return the releasable snapshot reference
     */
    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        final CatalogSnapshot currentSnapshot = catalogSnapshot;
        currentSnapshot.incRef(); // this should be package-private
        return new ReleasableRef<>(catalogSnapshot) {
            @Override
            public void close() throws Exception {
                currentSnapshot.decRef(); // this should be package-private
            }
        };
    }

    /**
     * Releasable reference wrapper.
     */
    public static abstract class ReleasableRef<T> implements AutoCloseable {
        private T t;

        /**
         * Creates a new releasable reference.
         * @param t the reference object
         */
        public ReleasableRef(T t) {
            this.t = t;
        }

        /**
         * Gets the reference.
         * @return the reference object
         */
        public T getRef() {
            return t;
        }
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return null;
    }

    @Override
    public TranslogManager translogManager() {
        return null;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return null;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, long fromSeqNo, long toSeqNo, boolean requiredFullRange, boolean accurateCount) throws IOException {
        return null;
    }

    @Override
    public String getHistoryUUID() {
        return "";
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        return 0;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return false;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return List.of();
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return 0;
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    @Override
    public long getLastWriteNanos() {
        return 0;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments, String forceMergeUUID) throws EngineException, IOException {

    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {

    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public ConfigurationProvider configProvider() {
        return null;
    }

    @Override
    public OperationMapper operationMapper() {
        return null;
    }

    @Override
    public void failEngine(String reason, Exception failure) {

    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public void maybePruneDeletes() {

    }

    @Override
    public boolean maybeRefresh(String source) {
        return false;
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {

    }

    @Override
    public GatedCloseable<CommitData> acquireSafeCommit() {
        return null;
    }
}
