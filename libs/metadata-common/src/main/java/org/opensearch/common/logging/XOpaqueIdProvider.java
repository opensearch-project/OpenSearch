/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.logging;

import java.util.function.Supplier;

/**
 * Provider for X-Opaque-Id header value. This class allows components like DeprecationLogger
 * to access X-Opaque-Id without directly depending on HeaderWarning or other classes like - ThreadContext.
 *
 * @opensearch.internal
 */
public class XOpaqueIdProvider {

    private static volatile Supplier<String> xOpaqueIdSupplier = () -> "";

    /**
     * Sets the supplier for X-Opaque-Id values.
     *
     * @param supplier the supplier that provides X-Opaque-Id values
     */
    public static void setXOpaqueIdSupplier(Supplier<String> supplier) {
        xOpaqueIdSupplier = supplier != null ? supplier : () -> "";
    }

    /**
     * Gets the current X-Opaque-Id value.
     *
     * @return the X-Opaque-Id value, or empty string if none available
     */
    public static String getXOpaqueId() {
        return xOpaqueIdSupplier.get();
    }
}
