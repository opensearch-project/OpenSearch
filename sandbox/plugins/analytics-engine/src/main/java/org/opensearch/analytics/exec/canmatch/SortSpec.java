/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

/**
 * Primary sort key of a {@code sort <field> | head N} shard fragment, extracted at
 * DAG-build time. The can-match phase uses it to request each shard's min/max for the
 * column and then dispatch the most promising shards first.
 *
 * <p>A non-null instance is also the eligibility signal: the pushed-down Sort it comes
 * from only exists for this query shape (see {@link SortSpecExtractor}).
 *
 * <p>Secondary sort keys are not modeled — a shard losing on the primary key loses
 * outright, and when primary keys tie, min/max can't separate the shards anyway.
 *
 * @param column     sort column name, as it appears in the fragment's row type
 * @param descending true for {@code DESC}, where a shard's best value is its {@code max}
 * @param limit      rows the coordinator must collect: {@code offset + fetch}, not {@code fetch}.
 *                   Offset rows are collected and then discarded, so budgeting {@code fetch} alone
 *                   would let a top-N gate think it was done while still short by {@code offset}.
 *                   Always {@code > 0}; {@link SortSpecExtractor} returns no spec otherwise.
 *
 * @opensearch.internal
 */
public record SortSpec(String column, boolean descending, int limit) {
}
