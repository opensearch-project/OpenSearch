/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;

import java.util.OptionalInt;

/**
 * Per-shard backend selection hint. The planner consults a backend's
 * {@code BackendShardPreference} when the same fragment has multiple viable backends, so the
 * backend can declare a preference score given the fragment shape and shard-local context.
 *
 * <p>Higher scores win. Returning {@link OptionalInt#empty()} means "no opinion" — the backend
 * is fine being treated as a generic alternative. The selector falls back to value-producing
 * backends in tie / no-opinion situations.
 *
 * <h2>Today's only consumer</h2>
 * <p>Lucene's count-fast-path: when {@link ShardPreferenceContext#preferMetadataDriver()} is
 * on AND the resolved fragment is a count Lucene can drive end-to-end (Aggregate over empty
 * group-set with COUNT-only calls), Lucene returns a positive score. Otherwise empty.
 *
 * <h2>Future consumers</h2>
 * <ul>
 *   <li>Deleted-doc routing — Lucene gets {@code liveDocs} masking for free; DataFusion
 *       needs explicit pushdown. When the shard has deletes, prefer Lucene.</li>
 *   <li>Cache warmth — when the OpenSearch query cache is hot for the predicate, Lucene
 *       wins on cache hits; DataFusion's columnar scan wins on cold cache.</li>
 *   <li>Segment count — single-segment shards favor Lucene; many small segments favor
 *       DataFusion's vectorized batching.</li>
 * </ul>
 *
 * <p>The context surface (today: just {@code preferMetadataDriver}) grows as new consumers
 * land; backends only need fields they actually use.
 *
 * @opensearch.internal
 */
public interface BackendShardPreference {

    /**
     * Score this backend's preference for executing {@code fragment} on the current shard.
     * Higher = stronger preference.
     *
     * @return preference score, or empty for "no opinion / not applicable"
     */
    OptionalInt scoreFor(RelNode fragment, ShardPreferenceContext ctx);
}
