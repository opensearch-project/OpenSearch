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
import java.util.Collections;
import java.util.Iterator;

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
public class LuceneSourceProvider implements SourceProvider<LuceneSourceContext, Object, DirectoryReader> {

    @Override
    public LuceneSourceContext createContext(Object query, DirectoryReader reader) throws IOException {
        return new LuceneSourceContext(query, reader);
    }

    @Override
    public Iterator<Object> execute(LuceneSourceContext context) throws IOException {
        // TODO: execute query via context.getSearcher(), collect results, return iterator
        return Collections.emptyIterator();
    }

    @Override
    public void close() {}
}
