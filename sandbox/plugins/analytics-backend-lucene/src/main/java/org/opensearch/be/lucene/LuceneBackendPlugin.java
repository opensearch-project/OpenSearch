/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.be.lucene.predicate.PredicateHandlerRegistry;
import org.opensearch.plugins.Plugin;

/**
 * Lucene text-search backend plugin for the analytics query engine.
 *
 * <p>Registers as an {@link AnalyticsSearchBackendPlugin} providing Lucene-based
 * predicate execution. Supported operators are derived from {@link PredicateHandlerRegistry}.
 */
public class LuceneBackendPlugin extends Plugin implements AnalyticsSearchBackendPlugin {

    @Override
    public String name() {
        return "lucene";
    }

    @Override
    public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
        return new LuceneSearchExecEngine();
    }
}
