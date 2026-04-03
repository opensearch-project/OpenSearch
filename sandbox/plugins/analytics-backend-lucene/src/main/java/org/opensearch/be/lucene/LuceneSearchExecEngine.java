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
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Lucene-backed search execution engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchExecEngine implements SearchExecEngine<ExecutionContext, EngineResultStream> {

    private final LuceneSearchContext context;

    public LuceneSearchExecEngine(LuceneSearchContext context) {
        this.context = context;
    }

    @Override
    public void prepare(ExecutionContext requestContext) {
        // TODO: extract query from plan and set on context
    }

    @Override
    public EngineResultStream execute(ExecutionContext requestContext) throws IOException {
        // TODO: execute via LuceneEngineSearcher and return result stream
        return null;
    }

    public LuceneSearchContext getContext() {
        return context;
    }

    @Override
    public void close() throws IOException {
        context.close();
    }
}
