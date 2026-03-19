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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.SourceContext;

import java.io.IOException;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSourceContext implements SourceContext {

    private final Object query;
    private final DirectoryReader reader;
    private final IndexSearcher searcher;

    public LuceneSourceContext(Object query, DirectoryReader reader) {
        this.query = query;
        this.reader = reader;
        this.searcher = new IndexSearcher(reader);
    }

    @Override
    public Object query() {
        return query;
    }

    public DirectoryReader getReader() {
        return reader;
    }

    public IndexSearcher getSearcher() {
        return searcher;
    }

    @Override
    public void close() throws IOException {}
}
