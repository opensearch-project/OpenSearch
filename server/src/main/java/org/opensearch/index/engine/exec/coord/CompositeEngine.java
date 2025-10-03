/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;


import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.bridge.Indexer;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExperimentalApi
public class CompositeEngine implements Indexer {

    private final CompositeIndexingExecutionEngine engine;
    private List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private CatalogSnapshot catalogSnapshot;
    private List<CatalogSnapshotAwareRefreshListener> catalogSnapshotAwareRefreshListeners = new ArrayList<>();
    private Map<org.opensearch.vectorized.execution.search.DataFormat, List<SearchExecEngine<?, ?, ?, ?>>> readEngines = new HashMap<>();

    public CompositeEngine(MapperService mapperService, PluginsService pluginsService, ShardPath shardPath) throws IOException {
        List<SearchEnginePlugin> searchEnginePlugins = pluginsService.filterPlugins(SearchEnginePlugin.class);
        // How to bring the Dataformat here? Currently this means only Text and LuceneFormat can be used
        this.engine = new CompositeIndexingExecutionEngine(mapperService, pluginsService, shardPath);

        // Refresh here so that catalog snapshot gets initialized
        // TODO : any better way to do this ?
        refresh("start");
        // TODO : how to extend this for Lucene ? where engine is a r/w engine
        // Create read specific engines for each format which is associated with shard
        for(SearchEnginePlugin searchEnginePlugin : searchEnginePlugins) {
            for(org.opensearch.vectorized.execution.search.DataFormat dataFormat : searchEnginePlugin.getSupportedFormats()) {
                List<SearchExecEngine<?, ?, ?, ?>> currentSearchEngines = readEngines.getOrDefault(dataFormat, new ArrayList<>());
                SearchExecEngine<?,?,?,?> newSearchEngine = searchEnginePlugin.createEngine(dataFormat,
                    Collections.emptyList(),
                    shardPath);

                currentSearchEngines.add(newSearchEngine);
                readEngines.put(dataFormat, currentSearchEngines);

                // TODO : figure out how to do internal and external refresh listeners
                // Maybe external refresh should be managed in opensearch core and plugins should always give
                // internal refresh managers
                // 60s as refresh interval -> ExternalReaderManager acquires a view every 60 seconds
                // InternalReaderManager -> IndexingMemoryController , it keeps on refreshing internal maanger
                //
                if(newSearchEngine.getRefreshListener(Engine.SearcherScope.INTERNAL) != null) {
                    catalogSnapshotAwareRefreshListeners.add(newSearchEngine.getRefreshListener(Engine.SearcherScope.INTERNAL));
                }
            }
        }
    }

    public SearchExecEngine<?,?,?,?> getReadEngine(org.opensearch.vectorized.execution.search.DataFormat dataFormat) {
        return readEngines.getOrDefault(dataFormat, new ArrayList<>()).getFirst();
    }

    public SearchExecEngine<?,?,?,?> getPrimaryReadEngine() {
        // Return the first available ReadEngine as primary
        return readEngines.values().stream()
            .filter(list -> !list.isEmpty())
            .findFirst()
            .map(list -> list.getFirst())
            .orElse(null);
    }

    public CompositeDataFormatWriter.CompositeDocumentInput documentInput() throws IOException {
        return engine.createWriter().newDocumentInput();
    }

    public Engine.IndexResult index(Engine.Index index) throws IOException {
        WriteResult writeResult = index.documentInput.addToWriter();
        // translog, checkpoint, other checks
        return new Engine.IndexResult(writeResult.version(), writeResult.seqNo(), writeResult.term(), writeResult.success());
    }


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
        CatalogSnapshot newCatSnap = null;
        try {
            RefreshResult refreshResult = engine.refresh(new RefreshInput());
            if (refreshResult == null) {
                return;
            }
            newCatSnap = new CatalogSnapshot(refreshResult, id + 1L);
            System.out.println("CATALOG SNAPSHOT: " + newCatSnap);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        newCatSnap.incRef();
        if (catalogSnapshot != null) {
            catalogSnapshot.decRef();
        }
        catalogSnapshot = newCatSnap;

        catalogSnapshotAwareRefreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true, catalogSnapshot);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        refreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public CatalogSnapshot catalogSnapshot() {
        return catalogSnapshot;
    }

    // This should get wired into searcher acquireSnapshot for initializing reader context later
    // this now becomes equivalent of the reader
    // Each search side specific impl can decide on how to init specific reader instances using this pit snapshot provided by writers
    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        catalogSnapshot.incRef(); // this should be package-private
        return new ReleasableRef<CatalogSnapshot>(catalogSnapshot) {
            @Override
            public void close() throws Exception {
                catalogSnapshot.decRef(); // this should be package-private
            }
        };
    }



    @ExperimentalApi
    public static abstract class ReleasableRef<T> implements AutoCloseable {
        private T t;

        public ReleasableRef(T t) {
            this.t = t;
        }

        public T getRef() {
            return t;
        }
    }

    public static void main(String[] args) throws Exception {
        CompositeEngine coordinator = new CompositeEngine(null, null, null);

        for (int i = 0; i < 5; i++) {

            // Ingestion into one generation
            for (int k = 0; k < 10; k++) {
                try (CompositeDataFormatWriter.CompositeDocumentInput doc = coordinator.documentInput()) {

                    // Mapper part
                    doc.addField(new KeywordFieldMapper.KeywordFieldType("f1"), k + "_v1");
                    doc.addField(new KeywordFieldMapper.KeywordFieldType("f2"), k + "_v2");
                    doc.addField(new KeywordFieldMapper.KeywordFieldType("f3"), k + "_v3");
                    doc.addField(new KeywordFieldMapper.KeywordFieldType("f4"), k + "_v4");
                    Engine.Index index = new Engine.Index(null, 1L, null);
                    index.documentInput = doc;

                    // applyIndexOperation part
                    coordinator.index(index);
                }
            }

            // Refresh until generation
            coordinator.refresh("_manual_test");
            System.out.println(coordinator.catalogSnapshot);
        }
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        return null;
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        return null;
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
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments, String forceMergeUUID) throws EngineException, IOException {

    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {

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
}
