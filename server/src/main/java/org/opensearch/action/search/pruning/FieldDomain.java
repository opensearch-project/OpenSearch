/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

/**
 * Index-level metadata describing the value domain of one field in one concrete index.
 *
 * A field domain is an index-side summary of the values that documents in the index may contain for a field. For
 * example, a date field domain can describe the minimum and maximum timestamp values present in the index, while a
 * future term domain could describe a trusted set of known keyword values. Search pruning compares this index-side
 * domain with a mandatory query constraint for the same field. If the two are provably disjoint, the whole shard group
 * for that concrete index can be skipped before can-match or query execution.
 *
 * Implementations are type-specific. Search pruning treats unknown implementations conservatively and keeps the
 * candidate shard group unless a registered evaluator can prove that the index cannot match the query constraint.
 * This is a correctness boundary: incomplete, stale, unsupported, or untrusted domains must not cause pruning.
 */
public interface FieldDomain {
    /**
     * Field name this domain describes.
     */
    String field();

    /**
     * Metadata type used to select the parser/evaluator implementation.
     */
    String type();

    /**
     * Whether this domain is trusted as complete for pruning.
     */
    boolean finalized();
}
