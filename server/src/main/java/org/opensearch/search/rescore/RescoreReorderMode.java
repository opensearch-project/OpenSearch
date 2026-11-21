/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.rescore;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Controls how hits are reordered after the rescore phase relative to hits that
 * were not rescored (hits beyond the rescore window).
 * <p>
 * Historically ({@link #LEGACY}) the rescored window was spliced back into the
 * collected hits and everything was re-sorted by score together. When a rescore
 * lowers the scores of the top hits, the rescored hits sink below non-rescored
 * hits and — because the set of collected hits depends on {@code from + size} —
 * pagination becomes inconsistent: the same documents can appear on multiple
 * pages while others are never returned.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public enum RescoreReorderMode implements Writeable {
    /**
     * The historical behavior: rescore the top {@code window_size} hits, then
     * re-sort them together with the non-rescored hits by score. Subject to
     * inconsistent pagination when the rescore lowers scores. Kept for backwards
     * compatibility.
     */
    LEGACY,
    /**
     * Rescore every collected hit (i.e. {@code max(from + size, window_size)}
     * hits per shard) instead of only the top {@code window_size}, producing a
     * single consistent ordering. {@code window_size} still sets the minimum
     * number of hits to rescore.
     */
    ALL_HITS,
    /**
     * Confine reordering to the rescore window: the top {@code window_size} hits
     * are reordered among themselves and always stay ahead of the non-rescored
     * hits, which keep their original order. To preserve this ordering across
     * shard result merging, window scores are uniformly shifted up when the
     * rescore would drop them below the non-rescored hits, so returned scores
     * may not equal the raw combined scores.
     */
    WINDOW_ONLY;

    public static RescoreReorderMode readFromStream(StreamInput in) throws IOException {
        return in.readEnum(RescoreReorderMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static RescoreReorderMode fromString(String reorderMode) {
        for (RescoreReorderMode mode : values()) {
            if (reorderMode.toLowerCase(Locale.ROOT).equals(mode.name().toLowerCase(Locale.ROOT))) {
                return mode;
            }
        }
        throw new IllegalArgumentException("illegal reorder_mode [" + reorderMode + "]");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
