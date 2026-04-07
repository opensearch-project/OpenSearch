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
 * Used by planner rules to determine which backend handles each operator.
 *
 * @opensearch.internal
 */
public enum OperatorCapability {
    SCAN,
    FILTER,
    /** Can evaluate filter predicates on derived/expression columns (e.g. HAVING on aggregate output). */
    FILTER_ON_EXPRESSIONS,
    AGGREGATE,
    SORT,
    JOIN,
    WINDOW,
    PROJECT,
    COORDINATOR_REDUCE
}
