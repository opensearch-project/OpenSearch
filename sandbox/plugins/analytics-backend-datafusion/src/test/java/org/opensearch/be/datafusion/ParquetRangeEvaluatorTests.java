/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.exec.canmatch.CanMatchFilterSerializer;
import org.opensearch.analytics.exec.canmatch.LongRange;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin.CanMatchResult;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Fail-open coverage for {@link ParquetRangeEvaluator} that needs no native library or real
 * shard. The happy path (an actual parquet fold) is exercised by the end-to-end IT and the
 * Rust-side {@code can_match} unit tests; these lock in the Java-side control flow and the
 * short-circuit contract that the design leans on.
 */
public class ParquetRangeEvaluatorTests extends OpenSearchTestCase {

    /**
     * No filters and no sort column: there is nothing to evaluate, so the evaluator must return
     * "match, no bounds" WITHOUT touching the shard — the reader-acquire cost is skipped entirely.
     * This is the common bare-`sort | head N` short-circuit before it even reaches a sort column.
     */
    public void testNoFiltersNoSortColumnShortCircuitsWithoutTouchingShard() {
        IndexShard shard = mock(IndexShard.class);
        DataFusionPlugin plugin = mock(DataFusionPlugin.class);

        CanMatchResult result = ParquetRangeEvaluator.evaluateWithBounds(shard, new byte[0], null, plugin);

        assertTrue("empty work must fail open to match", result.canMatch());
        assertNull(result.bounds());
        // The whole point of the short-circuit: no reader, no service, no shard interaction.
        verifyNoInteractions(shard);
    }

    /**
     * A sort column is requested but the DataFusion service is unavailable. The prune answer is
     * then a fail-open default (match), and no bounds can be produced.
     */
    public void testServiceUnavailableFailsOpen() throws Exception {
        IndexShard shard = mock(IndexShard.class);
        DataFusionPlugin plugin = mock(DataFusionPlugin.class);
        when(plugin.getDataFusionService()).thenReturn(null);

        byte[] filters = CanMatchFilterSerializer.serialize(List.of(new LongRange("latency", 0, 100)));
        CanMatchResult result = ParquetRangeEvaluator.evaluateWithBounds(shard, filters, "latency", plugin);

        assertTrue("unavailable service must not prune", result.canMatch());
        assertNull("no bounds when the probe could not run", result.bounds());
    }

    /**
     * The bounds-only path (a sort column, no filters) with the service unavailable must also fail
     * open rather than throw — a bare sort must never be penalised by a probe failure.
     */
    public void testBoundsOnlyServiceUnavailableFailsOpen() {
        IndexShard shard = mock(IndexShard.class);
        DataFusionPlugin plugin = mock(DataFusionPlugin.class);
        when(plugin.getDataFusionService()).thenReturn(null);

        CanMatchResult result = ParquetRangeEvaluator.evaluateWithBounds(shard, new byte[0], "latency", plugin);

        assertTrue(result.canMatch());
        assertNull(result.bounds());
    }

    /**
     * {@code evaluate(...)} is the boolean-only entry the shipped prune path uses. It must keep
     * delegating to the combined method and expose only the prune bit — never throw on a
     * degraded backend.
     */
    public void testLegacyEvaluateDelegatesAndFailsOpen() throws Exception {
        IndexShard shard = mock(IndexShard.class);
        DataFusionPlugin plugin = mock(DataFusionPlugin.class);
        when(plugin.getDataFusionService()).thenReturn(null);

        byte[] filters = CanMatchFilterSerializer.serialize(List.of(new LongRange("latency", 0, 100)));
        assertTrue(ParquetRangeEvaluator.evaluate(shard, filters, plugin));
    }
}
