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
 * Per-function serializer for delegated predicates. Registered by backends that accept
 * delegation, keyed by {@link ScalarFunction} in
 * {@link BackendCapabilityProvider#delegatedPredicateSerializers()}.
 *
 * <p>Each implementation knows how to extract field names and query parameters from a
 * {@link RexCall} and serialize them into backend-specific bytes that can be deserialized
 * at the data node to create the appropriate query.
 *
 * <p>A serializer is the single place that understands a predicate's shape, so it can also
 * report the fields the predicate references at planning time via {@link #referencedFields}.
 * This is used for full-text predicates whose fields may live inside an opaque query string
 * (e.g. {@code query_string}) rather than only in a {@code fields} operand. Serializers that do
 * not own such field-resolution semantics inherit the default (no field references reported).
 *
 * <p>TODO(same-backend-combining): When tree normalization combines adjacent same-backend
 * predicates under AND/OR into a single BooleanQuery, serializers will need to handle
 * composite predicate shapes — not just single-function leaves.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface DelegatedPredicateSerializer {

    /**
     * Serializes a delegated predicate into backend-specific bytes.
     *
     * @param call         the original RexCall expression (e.g., MATCH($1, 'hello world'))
     * @param fieldStorage per-column storage metadata; {@link org.apache.calcite.rex.RexInputRef}
     *                     indices in {@code call} index into this list
     * @return backend-specific serialized bytes
     */
    byte[] serialize(RexCall call, List<FieldStorageInfo> fieldStorage);

    /**
     * Returns the fields this predicate references, for plan-time validation, or {@code null} if
     * this serializer does not expose field references (the default). Runs at planning time and
     * must not require a {@code QueryShardContext} — field identity is analyzer-independent, so only
     * the predicate's {@link RexCall} and per-column {@link FieldStorageInfo} are needed.
     *
     * <p>Overridden by full-text serializers whose fields may be written inside the query string
     * (e.g. {@code query_string}); the implementation is expected to derive fields from the
     * <em>same</em> operand path it would use for {@link #serialize}, so extraction can never drift
     * from execution.
     *
     * @param call         the relevance {@link RexCall} (e.g. {@code QUERY_STRING(MAP('fields', ...), MAP('query', ...))})
     * @param fieldStorage per-column storage metadata; {@link org.apache.calcite.rex.RexInputRef}
     *                     indices in {@code call} index into this list
     * @return the referenced fields split into validated literals vs. passed-through patterns, plus
     *         lenient metadata, or {@code null} if field references are not exposed
     */
    default FieldReferences referencedFields(RexCall call, List<FieldStorageInfo> fieldStorage) {
        return null;
    }
}
