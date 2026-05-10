/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Relational operations that a backend may support.
 *
 * <p>Only operators without dedicated per-function capability classes are listed here.
 * Operators with dedicated classes (FILTER → {@link FilterCapability}, AGGREGATE →
 * {@link AggregateCapability}, PROJECT → {@link ProjectCapability}, SCAN →
 * {@link ScanCapability}) are declared via those classes — a non-empty capability set
 * implies operator support.
 *
 * @opensearch.internal
 */
public enum EngineCapability {
    SORT,
    UNION,
    /**
     * Window-function evaluation inside a Project expression. A backend that declares this
     * capability commits to evaluating any {@link org.apache.calcite.rex.RexOver} encountered in a
     * project list — i.e. window aggregates such as {@code ROW_NUMBER() OVER (PARTITION BY ...
     * ORDER BY ...)} that PPL's {@code top}/{@code rare}/{@code streamstats} commands lower to.
     * The capability is intentionally coarse (no per-window-function granularity): the substrait
     * standard catalog already constrains which window aggregates a backend's substrait consumer
     * can decode, and failing the runtime decode is a clearer error than spreading a per-function
     * registry across SPI plus backend.
     */
    WINDOW
}
