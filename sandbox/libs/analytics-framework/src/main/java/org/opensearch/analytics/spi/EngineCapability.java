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
 * <p>TODO: COORDINATOR_REDUCE will be replaced by a DataTransferCapability-based declaration.
 * The underlying capability is: a backend can accept Arrow Record Batches as input and execute
 * its supported operators over them — regardless of where this happens. At the coordinator,
 * this means accepting partial results for final reduce. At the data node, this means accepting
 * Arrow batches from another backend (e.g. Lucene streaming doc values into DataFusion for
 * aggregation). Same capability, different scope.
 *
 * @opensearch.internal
 */
public enum EngineCapability {
    SORT,
    COORDINATOR_REDUCE
}
