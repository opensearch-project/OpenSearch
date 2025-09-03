/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.datafusion.DataFusionService;
import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.search.aggregations.SearchResultsCollector;

import java.io.IOException;
import java.util.List;

public class DatafusionSearcher implements EngineSearcher {
    private final String source;

    public DatafusionSearcher(String source) {
        this.source = source;
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public void search(byte[] substraitInput, List<SearchResultsCollector<?>> collectors) throws IOException {
        // TODO : call search here to native
    }

    @Override
    public void close() {

    }
}
