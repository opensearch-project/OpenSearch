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
    VALUES
}
