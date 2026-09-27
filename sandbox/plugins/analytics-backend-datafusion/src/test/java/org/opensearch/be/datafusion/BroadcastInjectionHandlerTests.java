/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link BroadcastInjectionHandler}'s fail-fast paths.
 *
 * <p>The happy path (decode IPC → register memtable → return session state) requires a live
 * DataFusion session, which in turn requires a non-null {@code DatafusionReader} — the
 * {@code createSessionContext} FFM entry validates reader ≠ 0. Spinning that up in a unit test
 * requires a parquet file on disk plus shard-level fixtures. The native round trip for
 * {@code registerMemtable} is already covered by {@link DatafusionMemtableReduceSinkTests}
 * (same FFM call, same pointer hand-off contract); end-to-end verification for the broadcast
 * handler lives in the integration test added alongside Commit 6.
 *
 * <p>What this suite pins: the handler refuses to run when the required predecessor
 * ({@code ShardScanInstructionHandler}) has not produced a {@link DataFusionSessionState}.
 */
public class BroadcastInjectionHandlerTests extends OpenSearchTestCase {

    public void testMissingSessionStateThrows() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastInjectionHandler handler = new BroadcastInjectionHandler();
            BroadcastInjectionInstructionNode node = new BroadcastInjectionInstructionNode("broadcast-0", 0, new byte[0]);
            ShardScanExecutionContext ctx = new ShardScanExecutionContext("t", null, null);
            ctx.setAllocator(alloc);

            // null backend context — classic "instruction inserted before scan-setup" bug.
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> handler.apply(node, ctx, null));
            assertTrue("error must name the expected producer", ex.getMessage().contains("ShardScanInstructionHandler"));

            // Wrong backend-context type.
            ex = expectThrows(IllegalStateException.class, () -> handler.apply(node, ctx, new DummyContext()));
            assertTrue("error must name the actual type", ex.getMessage().contains("DummyContext"));
        }
    }

    private static final class DummyContext implements org.opensearch.analytics.spi.BackendExecutionContext {
        @Override
        public void close() {}
    }
}
