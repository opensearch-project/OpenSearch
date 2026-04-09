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

import java.io.IOException;

/**
 * Lucene implementation of {@link SearchExecEngine}.
 *
 * <p>Bridges the upstream execution interface to the Lucene search path:
 * <ul>
 *   <li>{@link #prepare} — extracts DirectoryReader from ExecutionContext</li>
 *   <li>{@link #execute} — delegates Lucene query execution through
 *       {@link LuceneIndexFilterProvider} and packages results</li>
 *   <li>{@link #close} — releases cached resources</li>
 * </ul>
 *
 * <p>This is the single search execution path for Lucene in the analytics engine.
 * {@link LuceneIndexFilterProvider} handles per-segment collection;
 * this class orchestrates them through {@link LuceneFilterExecutor}.
 */
public class LuceneSearchExecEngine implements SearchExecEngine<ExecutionContext, EngineResultStream> {

    private final LuceneFilterExecutor bridge = new LuceneFilterExecutor();

    /**
     * Returns the bridge for direct access to planner-side methods
     * ({@link LuceneFilterExecutor#convertFragment}) and segment-level
     * execution ({@link LuceneFilterExecutor#executeForSegment}).
     */
    public LuceneFilterExecutor getBridge() {
        return bridge;
    }

    @Override
    public void prepare(ExecutionContext context) {
        // The DirectoryReader will be extracted from ExecutionContext.getReader()
        // using the Lucene DataFormat when the full execution path is wired.
        // For now, the bridge can be initialized separately via getBridge().initialize()
        // when a DefaultShardExecutionContext is available.
    }

    @Override
    public EngineResultStream execute(ExecutionContext context) throws IOException {
        // TODO: extract delegated Lucene predicates (LUCENE_DELEGATED byte[] payloads)
        //       from the execution context, run them through the bridge using the
        //       directoryReader acquired in prepare(), return as EngineResultStream.
        //       Blocked on: QueryShardContext availability + predicate payload in context.
        return null;
    }

    @Override
    public void close() throws IOException {
        bridge.close();
    }
}
