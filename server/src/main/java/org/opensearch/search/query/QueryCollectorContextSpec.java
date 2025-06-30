/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * interface of QueryCollectorContextSpec
 */
@ExperimentalApi
public interface QueryCollectorContextSpec {
    /**
     * Context name for QueryCollectorContext
     * @return string of context name
     */
    String getContextName();

    /**
     * Create collector
     * @param in collector
     * @return collector
     * @throws IOException
     */
    Collector create(Collector in) throws IOException;

    /**
     * Create collector manager
     * @param in collector manager
     * @return collector manager
     * @throws IOException
     */
    CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException;

    /**
     * Post process query result
     * @param result query result
     * @throws IOException
     */
    void postProcess(QuerySearchResult result) throws IOException;
}
