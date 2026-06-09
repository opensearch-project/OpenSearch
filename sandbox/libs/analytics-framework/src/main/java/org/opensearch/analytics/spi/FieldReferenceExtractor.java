/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rex.RexCall;

import java.util.List;

/**
 * Per-function extractor for the fields a relevance predicate explicitly references. Registered by
 * backends that own a query language's field-resolution semantics, keyed by {@link ScalarFunction}
 * in {@link BackendCapabilityProvider#fieldReferenceExtractors()}.
 *
 * <p>Unlike {@link DelegatedPredicateSerializer} (which produces execution bytes), this runs at
 * planning time and returns the referenced fields so the planner can validate field types. The
 * implementation is expected to derive fields from the <em>same</em> {@code QueryBuilder}
 * construction it would use for execution, so extraction can never drift from execution.
 *
 * <p>Extraction must not require a {@code QueryShardContext}: field identity is independent of
 * analyzers, so only the predicate's {@link RexCall} and the per-column
 * {@link FieldStorageInfo} are needed.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface FieldReferenceExtractor {

    /**
     * Returns the fields a relevance predicate references.
     *
     * @param call         the relevance {@link RexCall} (e.g. {@code QUERY_STRING(MAP('fields', ...), MAP('query', ...))})
     * @param fieldStorage per-column storage metadata; {@link org.apache.calcite.rex.RexInputRef}
     *                     indices in {@code call} index into this list
     * @return the referenced fields split into validated literals vs. passed-through patterns,
     *         plus fan-out and lenient metadata
     */
    FieldReferences referencedFields(RexCall call, List<FieldStorageInfo> fieldStorage);
}
