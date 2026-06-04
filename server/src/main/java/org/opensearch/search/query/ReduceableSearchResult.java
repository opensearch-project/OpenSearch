/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * The search result callback returned by reduce phase of the collector manager.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.0.0")
public interface ReduceableSearchResult {
    /**
     * Apply the reduce operation to the query search results
     * @param result query search results
     * @throws IOException exception if reduce operation failed
     */
    void reduce(QuerySearchResult result) throws IOException;
}
