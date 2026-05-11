/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;

import java.util.List;
import java.util.Map;

/**
 * Per-engine accessor for delete state (currently: live-docs bitsets per segment).
 * Constructed once at {@link IndexingExecutionEngine} initialization.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Deleter {

    /**
     * Returns a point-in-time snapshot of per-segment live-docs bitsets.
     * Keyed by {@link Segment#generation()}; values use Lucene
     * {@code FixedBitSet#getBits()} layout. Absent key = all rows alive.
     */
    Map<Long, long[]> getLiveDocs(List<Segment> segments);
}
