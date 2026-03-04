/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Set;

/**
 * Lucene-backed {@link SearchExecEngine}.
 * Plan type is {@link Query}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchExecEngine implements SearchExecEngine<LuceneSearchContext, Query> {

    private final LuceneReaderManager readerManager;
    private long nextContextId;

    public LuceneSearchExecEngine() {
        this.readerManager = new LuceneReaderManager(getLuceneDataFormat());
    }

    private static DataFormat getLuceneDataFormat() {
        return new DataFormat() {
            @Override
            public String name() {
                return "Lucene";
            }

            @Override
            public long priority() {
                return 0;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
    }

    // TODO : replace this with filter provider/delegate methods
    @Override
    public void execute(LuceneSearchContext context) throws IOException {
        DirectoryReader reader = readerManager.getReader(context.catalogSnapshot());
        LuceneEngineSearcher searcher = new LuceneEngineSearcher(new IndexSearcher(reader), reader);
        try {
            searcher.search(context);
        } finally {
            searcher.close();
        }
    }

    @Override
    public LuceneSearchContext createContext(
        CatalogSnapshot snapshot,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        SearchShardTask task
    ) throws IOException {
        return new LuceneSearchContext(snapshot, request, shardTarget, readerManager);
    }

    @Override
    public Query convertFragment(Object fragment) {
        // DQE passes a Lucene Query directly
        if (fragment instanceof Query) {
            return (Query) fragment;
        }
        throw new UnsupportedOperationException("Expected Lucene Query, got " + fragment.getClass().getSimpleName());
    }

    @Override
    public EngineReaderManager<?> getReaderManager() {
        return null;
    }
}
