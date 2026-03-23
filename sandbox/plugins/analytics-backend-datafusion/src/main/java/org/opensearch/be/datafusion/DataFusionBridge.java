/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.backend.EngineBridge;

import java.io.IOException;

/**
 * Per-query bridge that wraps {@link DatafusionSearchExecEngine}.
 * Bound to a reader at construction — hides the {@link DatafusionContext}
 * from the analytics engine layer.
 */
public class DataFusionBridge implements EngineBridge<byte[], DatafusionResultStream, RelNode> {

    private final DatafusionSearchExecEngine engine;
    private final DatafusionReader reader;
    private DatafusionContext context;

    public DataFusionBridge(DatafusionSearchExecEngine engine, DatafusionReader reader) {
        this.engine = engine;
        this.reader = reader;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        return engine.convertFragment(fragment);
    }

    @Override
    public DatafusionResultStream execute(byte[] plan) {
        try {
            context = engine.createContext(reader, plan, null, null, null);
            return engine.execute(context);
        } catch (IOException e) {
            throw new RuntimeException("DataFusion execution failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (context != null) {
            context.close();
            context = null;
        }
    }
}
