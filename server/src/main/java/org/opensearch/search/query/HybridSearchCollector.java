/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopDocs;

import java.util.List;

/**
 * Common interface class for Hybrid search collectors
 */
public interface HybridSearchCollector extends Collector {
    /**
     * @return List of topDocs which contains topDocs of individual subqueries.
     */
    List<? extends TopDocs> topDocs();

    /**
     * @return count of total hits per shard
     */
    int getTotalHits();

    /**
     * @return maxScore found on a shard
     */
    float getMaxScore();
}
