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
}
