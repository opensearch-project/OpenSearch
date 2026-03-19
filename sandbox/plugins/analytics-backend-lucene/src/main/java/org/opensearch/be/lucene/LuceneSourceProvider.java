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
        throw new UnsupportedOperationException("Lucene source execution not yet implemented");
    }

    @Override
    public Object next(LuceneSourceContext context, Object stream) throws IOException {
        throw new UnsupportedOperationException("Lucene source streaming not yet implemented");
    }

    @Override
    public void close() {}
}
