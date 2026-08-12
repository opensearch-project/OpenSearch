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
 * Result of a {@link DslCalciteGrammar#validate} pass over a {@code SearchSourceBuilder}.
 *
 * <p>A request is {@link #supported()} when the grammar accepts every node in the DSL tree.
 * Otherwise {@link #rejectionReasons()} enumerates the reason codes that caused rejection
 * (e.g. {@code "script"}, {@code "range.time_zone"}).
 *
 * <p>Callers use this record to route the request:
 * <pre>{@code
 * RouteDecision decision = grammar.validate(source);
 * if (decision.supported()) {
 *     // Calcite path
 * } else {
 *     // codec path; log decision.rejectionReasons() for observability
 * }
 * }</pre>
 */
public record RouteDecision(boolean supported, List<String> rejectionReasons) {

    /** Cached instance for the common "everything is fine" result. */
    private static final RouteDecision SUPPORTED = new RouteDecision(true, Collections.emptyList());

    /**
     * Returns the singleton supported decision. Named {@code accepted} rather than
     * {@code supported} to avoid colliding with the record's own {@code supported()}
     * accessor (Java rejects a static factory with the same signature as an accessor).
     */
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
