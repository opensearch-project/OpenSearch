/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Index-level metadata describing the value domain of one field in one concrete index.
 *
 * A field domain is an index-side summary of values that documents in the index may contain for a field. For example,
 * a date field domain can describe the minimum and maximum timestamp values present in the index, while a future term
 * domain could describe a trusted set of known keyword values.
 *
 * The metadata is intentionally generic. Different OpenSearch features may use it for different purposes, such as
 * routing decisions, request planning, or search optimizations. Implementations are type-specific, and consumers are
 * responsible for interpreting only the domain types they understand. Incomplete, stale, unsupported, or untrusted
 * domains must be treated conservatively by consumers.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
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
     * Whether this domain is trusted as complete for consumers that require closed index-level value metadata.
     */
    boolean finalized();
}
