/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.PartialAggregateInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

/**
 * Handles PartialAggregate instruction on the shard side: prepares the partial-aggregate
 * physical plan on the already-open SessionContext created by the preceding ShardScan handler.
 *
 * <p>Calls {@link NativeBridge#preparePartialPlan(long, byte[])} which sets the Rust-side
 * aggregate mode to Partial and stores the prepared plan on the session handle for later
 * execution.
 */
public class PartialAggregateInstructionHandler implements FragmentInstructionHandler<PartialAggregateInstructionNode> {

    @Override
    public BackendExecutionContext apply(
        PartialAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext ctx = (ShardScanExecutionContext) commonContext;
        DataFusionSessionState state = (DataFusionSessionState) backendContext;
        long sessionPtr = state.sessionContextHandle().getPointer();
        NativeBridge.preparePartialPlan(sessionPtr, ctx.getFragmentBytes());
        return backendContext;
    }
}
