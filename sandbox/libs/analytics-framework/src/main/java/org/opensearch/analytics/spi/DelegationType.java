/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Types of work that can be delegated between backends.
 *
 * <p>A backend declares {@code canDelegate(FILTER)} if it has a custom
 * physical operator that can call into Analytics Core's delegation API
 * to offload filter predicates to another backend.
 *
 * <p>A backend declares {@code canAcceptDelegation(FILTER)} if it can
 * receive a delegated filter request (e.g. a serialized QueryBuilder)
 * and return results (e.g. a bitset of matching docIds).
 *
 * @opensearch.internal
 */
public enum DelegationType {
    /** Delegate a filter predicate → receive a bitset of matching docIds. */
    FILTER,
    /** Delegate doc value reads → receive a VectorSchemaRoot. */
    SCAN,
    /** Delegate an aggregate function (e.g. painless) → receive aggregated result. */
    AGGREGATE,
    /** Delegate a projection expression (e.g. painless script, highlighting) → receive projected columns. */
    PROJECT
}
