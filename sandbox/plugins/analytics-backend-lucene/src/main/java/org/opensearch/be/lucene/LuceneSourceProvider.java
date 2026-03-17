/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.SourceProvider;

import java.io.IOException;

/**
 * Lucene-backed {@link SourceProvider}.
 * <p>
 * Executes the full query+scan+filter in Lucene and streams back
 * projections/aggregation results to the primary engine (DataFusion).
 * <p>
 * Used when all queried fields are Lucene-indexed and Lucene can
 * fully resolve the query more efficiently than scanning parquet.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSourceProvider implements SourceProvider<LuceneSourceContext, Object> {

    @Override
    public LuceneSourceContext createContext(Object query, Object reader) throws IOException {
        return new LuceneSourceContext(query, (DirectoryReader) reader);
    }

    @Override
    public Object execute(LuceneSourceContext context) throws IOException {
        // TODO: execute query via context.getSearcher(), collect results, return stream handle
        throw new UnsupportedOperationException("Lucene source execution not yet implemented");
    }

    @Override
    public Object next(LuceneSourceContext context, Object stream) throws IOException {
        // TODO: pull next batch (Arrow VectorSchemaRoot) from stream
        throw new UnsupportedOperationException("Lucene source streaming not yet implemented");
    }

    @Override
    public void close() {}
}
