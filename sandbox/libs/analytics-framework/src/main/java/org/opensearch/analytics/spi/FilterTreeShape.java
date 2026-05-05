/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Backend-agnostic description of the boolean tree shape when filter delegation is active.
 * Provided by the planner so backends can choose their execution strategy without
 * re-inspecting the Substrait plan.
 *
 * @opensearch.internal
 */
public enum FilterTreeShape {
    /**
     * All predicates (delegated + native) are under a single AND — no interleaving
     * under OR/NOT. Backend can handle delegated bitsets and native predicates independently.
     */
    SINGLE_AND,
    /**
     * Delegated and native predicates are interleaved under OR/NOT — the boolean tree
     * mixes predicates from different backends under non-AND operators. Backend needs a
     * tree evaluator to combine bitsets from both backends per the boolean structure.
     */
    MIXED_BOOLEAN
}
