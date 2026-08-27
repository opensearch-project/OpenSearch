/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.router;

import java.util.Collections;
import java.util.List;

/**
 * Result of {@link DslCalciteGrammar#validate}: {@link #supported()} when the grammar accepts the
 * whole DSL tree, else {@link #rejectionReasons()} carries the reason codes (e.g.
 * {@code "query:match"}, {@code "terms.boost"}) and the caller routes to codec.
 */
public record RouteDecision(boolean supported, List<String> rejectionReasons) {

    /** Cached "supported" result. */
    private static final RouteDecision SUPPORTED = new RouteDecision(true, Collections.emptyList());

    /** The shared supported decision (named {@code accepted} to avoid clashing with the {@code supported()} accessor). */
    public static RouteDecision accepted() {
        return SUPPORTED;
    }

    /**
     * Constructs a rejection with the given reason codes.
     *
     * @param rejectionReasons the reason codes accumulated during the walk
     */
    public static RouteDecision rejected(List<String> rejectionReasons) {
        return new RouteDecision(false, List.copyOf(rejectionReasons));
    }
}
