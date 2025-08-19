/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;


import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.mapper.KeywordFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexingExecutionCoordinator {

    private final CompositeIndexingExecutionEngine engine;
    private List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private CatalogSnapshot catalogSnapshot;

    public IndexingExecutionCoordinator(/*MapperService mapperService, EngineConfig engineConfig*/) {
        this.engine = new CompositeIndexingExecutionEngine(null, new Any(List.of(DataFormat.TEXT)));
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

        refreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
        IndexingExecutionCoordinator coordinator = new IndexingExecutionCoordinator();

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
