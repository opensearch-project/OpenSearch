/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * DataFusion-backed search execution engine.
 * <p>
 * Delegates execution to the native DataFusion runtime via {@link DatafusionSearcher}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine {

    private final DatafusionContext datafusionContext;

    /**
     * Creates an execution engine backed by the given DataFusion context.
     * @param datafusionContext the DataFusion execution context
     */
    public DatafusionSearchExecEngine(DatafusionContext datafusionContext) {
        this.datafusionContext = datafusionContext;
    }

    @Override
    public void prepare(ExecutionContext requestContext) {
        // TODO: wire Substrait conversion (RelNode → Substrait bytes)
        byte[] substraitBytes = null;
        datafusionContext.setDatafusionQuery(new DatafusionQuery(requestContext.getTableName(), substraitBytes));
    }

    @Override
    public EngineResultStream execute(ExecutionContext requestContext) throws IOException {
        DatafusionSearcher searcher = datafusionContext.getSearcher();
        searcher.search(datafusionContext);
        return new DatafusionResultStream(datafusionContext.getStreamHandle());
    }

    @Override
    public void close() throws IOException {
        datafusionContext.close();
    }
}
