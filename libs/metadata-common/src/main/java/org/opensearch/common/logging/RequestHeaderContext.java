/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.logging;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;

/**
 * Utility class to hold request-level HTTP headers (like X-Opaque-Id)
 * using a supplier-based approach. This allows access to headers across components
 * (such as {@link DeprecationLogger}) without injecting ThreadContext explicitly.
 *
 * @opensearch.internal
 */
public class RequestHeaderContext {

    public static final String X_OPAQUE_ID = "X-Opaque-Id";

    /**
     * This is set once by the {@code Node} constructor, but it uses {@link CopyOnWriteArraySet} to ensure that tests can run in parallel.
     * <p>
     * Integration tests will create separate nodes within the same classloader, thus leading to a shared, {@code static} state.
     * In order for all tests to appropriately be handled, this must be able to remember <em>all</em>
     * {@code Header suppliers} that it is given in a thread safe manner.
     * <p>
     * For actual usage, multiple nodes do not share the same JVM and therefore this will only be set once in practice.
     */
    private static final CopyOnWriteArraySet<Supplier<Map<String, String>>> HEADER_SUPPLIER = new CopyOnWriteArraySet<>();

    public static void setHeaderSupplier(Supplier<Map<String, String>> headerSupplier) {
        HEADER_SUPPLIER.add(headerSupplier);
    }

    public static void removeHeaderSupplier(Supplier<Map<String, String>> headerSupplier) {
        HEADER_SUPPLIER.remove(headerSupplier);
    }

    public static String getXOpaqueId() {
        return getHeader(X_OPAQUE_ID);
    }

    public static String getHeader(String headerName) {
        return HEADER_SUPPLIER.stream()
            .map(Supplier::get)
            .map(headers -> headers.get(headerName))
            .filter(Objects::nonNull)
            .findFirst()
            .orElse("");
    }
}
