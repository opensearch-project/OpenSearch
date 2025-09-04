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
import org.opensearch.index.engine.ReadEngine;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExperimentalApi
public class IndexingExecutionCoordinator {

    private final CompositeIndexingExecutionEngine engine;
    private List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private CatalogSnapshot catalogSnapshot;
    private List<CatalogSnapshotAwareRefreshListener> catalogSnapshotAwareRefreshListeners = new ArrayList<>();
    private Map<org.opensearch.vectorized.execution.search.DataFormat, List<ReadEngine<?, ?, ?, ?, ?>>> readEngines = new HashMap<>();

    public IndexingExecutionCoordinator(MapperService mapperService, PluginsService pluginsService) throws IOException {
        List<SearchEnginePlugin> searchEnginePlugins = pluginsService.filterPlugins(SearchEnginePlugin.class);
        this.engine = new CompositeIndexingExecutionEngine(pluginsService, new Any(List.of(DataFormat.TEXT)));

        // Refresh here so that catalog snapshot gets initialized
        // TODO : any better way to do this ?
        refresh("start");
        // TODO : how to extend this for Lucene ? where engine is a r/w engine
        // Create read specific engines for each format which is associated with shard
        for(SearchEnginePlugin<?,?,?> searchEnginePlugin : searchEnginePlugins) {
            for(org.opensearch.vectorized.execution.search.DataFormat dataFormat : searchEnginePlugin.getSupportedFormats()) {
                ReadEngine<?,?,?,?,?> readEngine = searchEnginePlugin.createEngine(dataFormat,
                    catalogSnapshot.getSearchableFiles(dataFormat.toString()));
                readEngines.getOrDefault(dataFormat, new ArrayList<>()).add(readEngine);
                // TODO : figure out how to do internal and external refresh listeners
                // Maybe external refresh should be managed in opensearch core and plugins should always give
                // internal refresh managers
                // 60s as refresh interval -> ExternalReaderManager acquires a view every 60 seconds
                // InternalReaderManager -> IndexingMemoryController , it keeps on refreshing internal maanger
                //
                if(readEngine.getRefreshListener(Engine.SearcherScope.INTERNAL) != null) {
                    catalogSnapshotAwareRefreshListeners.add(readEngine.getRefreshListener(Engine.SearcherScope.INTERNAL));
                }
            }
        }
    }

    public ReadEngine<?,?,?,?,?> getReadEngine(org.opensearch.vectorized.execution.search.DataFormat dataFormat) {
        return readEngines.getOrDefault(dataFormat, new ArrayList<>()).getFirst();
    }

    public ReadEngine<?,?,?,?,?> getPrimaryReadEngine() {
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

    public Engine.IndexResult index(Engine.Index index) throws Exception {
        WriteResult writeResult = index.documentInput.addToWriter();
        // translog, checkpoint, other checks
        return new Engine.IndexResult(writeResult.version(), writeResult.seqNo(), writeResult.term(), writeResult.success());
    }


    public synchronized void refresh(String source) throws EngineException, IOException {
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
        IndexingExecutionCoordinator coordinator = new IndexingExecutionCoordinator(null, null);

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

}
