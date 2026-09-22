/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.BackendShardPreference;
import org.opensearch.analytics.spi.ShardPreferenceContext;

import java.util.OptionalInt;

/**
 * Lucene's per-shard preference: opt in to drive count-fast-path fragments when the user has
 * enabled {@code analytics.planner.prefer_metadata_driver}.
 *
 * <p>Today the only signal is the cluster setting + fragment shape. Future shard-local
 * inputs (deletes, segment count, query-cache warmth) plug into the same scoring function as
 * {@link ShardPreferenceContext} grows.
 *
 * @opensearch.internal
 */
final class LuceneShardPreference implements BackendShardPreference {

    /** Wants-to-drive score — beats generic alternatives (score 0). */
    private static final int COUNT_FAST_PATH_SCORE = 100;

    /** Veto score — actively don't pick this plan. Lucene returns this when the fragment
     *  isn't a count-fast-path so the selector doesn't accidentally collapse to a non-drivable
     *  Lucene alternative just because it appeared first in PlanForker order. */
    private static final int NOT_DRIVABLE_SCORE = -1;

    @Override
    public OptionalInt scoreFor(RelNode fragment, ShardPreferenceContext ctx) {
        if (ctx.preferMetadataDriver() == false) return OptionalInt.empty();
        if (LuceneFragmentConvertor.isCountFastPath(fragment) == false) {
            return OptionalInt.of(NOT_DRIVABLE_SCORE);
        }
        return OptionalInt.of(COUNT_FAST_PATH_SCORE);
    }
}
