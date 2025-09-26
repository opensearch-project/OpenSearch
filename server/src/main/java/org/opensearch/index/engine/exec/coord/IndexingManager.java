/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.LuceneCommitEngine;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;

public class IndexingManager {

    private final CompositeIndexingExecutionEngine engine;
    private final List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private final Committer committer;
    private CatalogSnapshot catalogSnapshot;

    public IndexingManager(Path indexPath, MapperService mapperService/*, EngineConfig engineConfig*/)
        throws IOException {
        this.engine = new CompositeIndexingExecutionEngine(mapperService, null, new Any(List.of(DataFormat.TEXT)), null,
            0);
        this.committer = new LuceneCommitEngine(indexPath);
    }

    public CompositeDataFormatWriter.CompositeDocumentInput documentInput() throws IOException {
        return engine.createCompositeWriter().newDocumentInput();
    }

    public Engine.IndexResult index(Engine.Index index) throws Exception {
        WriteResult writeResult = index.documentInput.addToWriter();
        // translog, checkpoint, other checks
        return new Engine.IndexResult(writeResult.version(), writeResult.seqNo(), writeResult.term(),
            writeResult.success());
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
        return new ReleasableRef<>(catalogSnapshot) {
            @Override
            public void close() throws Exception {
                catalogSnapshot.decRef(); // this should be package-private
            }
        };
    }

    public static abstract class ReleasableRef<T> implements AutoCloseable {

        private final T t;

        public ReleasableRef(T t) {
            this.t = t;
        }

        public T getRef() {
            return t;
        }
    }

    public static void main(String[] args) throws Exception {
        IndexingManager coordinator = new IndexingManager(
            Path.of("/Users/shnkgo/Downloads/mustang/lucene-committer-index/"), null);

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
