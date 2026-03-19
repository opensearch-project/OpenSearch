/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * @param <Q> the query type (e.g. Lucene Query)
 * @param <C> the context type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexFilterProvider<Q, C extends IndexFilterContext> extends Closeable {

    C createContext(Q query, Object reader) throws IOException;

    int createCollector(C context, int segmentOrd, int minDoc, int maxDoc);

    long[] collectDocs(C context, int collectorKey, int minDoc, int maxDoc);

    void releaseCollector(C context, int collectorKey);
}
