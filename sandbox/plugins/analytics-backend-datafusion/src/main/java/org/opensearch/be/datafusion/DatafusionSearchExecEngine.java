/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Iterator;

@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<DatafusionContext, byte[]> {

    private final long runtimePtr;

    public DatafusionSearchExecEngine(long runtimePtr) {
        this.runtimePtr = runtimePtr;
    }

    @Override
    public DatafusionContext createContext(Object reader, ShardSearchRequest request,
            SearchShardTarget shardTarget, SearchShardTask task) {
        DatafusionReader nativeReader = (DatafusionReader) reader;
        return new DatafusionContext(request, shardTarget, nativeReader.getPtr(), runtimePtr, task != null ? task.getId() : 0L);
    }

    @Override
    public byte[] convertFragment(Object fragment) {
        return SandboxDataFusionBridge.convertRelNode((org.apache.calcite.rel.RelNode) fragment);
    }

    /**
     * Executes the plan and returns an EngineResultStream (same as bridge path).
     * The Iterator wraps a single EngineResultStream element.
     */
    @Override
    public Iterator<?> executePlan(byte[] plan, DatafusionContext context) {
        context.setSubstraitPlan(plan);
        context.setTableName(SandboxDataFusionBridge.extractTableNameFromBytes(plan));
        try {
            context.executeQuery();
        } catch (IOException e) {
            throw new RuntimeException("Query execution failed", e);
        }
        // Wrap the native stream as EngineResultStream — same type as bridge returns
        EngineResultStream resultStream = new SandboxDataFusionBridge.ResultStream(
            context.getStreamPtr(), runtimePtr,
            new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE)
        );
        return java.util.Collections.singletonList(resultStream).iterator();
    }

    @Override
    public void execute(DatafusionContext context) throws IOException {
        context.executeQuery();
    }
}
