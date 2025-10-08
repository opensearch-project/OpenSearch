/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Streams TopDocs progressively from a shard.
 *
 * @opensearch.internal
 */
public interface ShardTopDocsStreamer {

    /**
     * Start streaming.
     */
    void onStart(SearchContext context);

    /**
     * Create leaf collector for segment.
     */
    LeafCollector newLeafCollector(LeafReaderContext leafContext, Scorable scorable) throws IOException;

    /**
     * Process document.
     */
    void onDoc(int globalDocId, float score, Object[] sortValues) throws IOException;

    /**
     * Maybe emit partial result.
     */
    void maybeEmit(boolean force) throws IOException;

    /**
     * Build final TopDocs.
     */
    TopDocsAndMaxScore buildFinalTopDocs() throws IOException;

    /**
     * Finish streaming.
     */
    void onFinish();

    int getCollectedCount();

    int getEmissionCount();

    boolean isCancelled();
}
