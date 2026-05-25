/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Covers the default-method body of {@link ArrowAllocatorPlugin#getNativeAllocatorStatsSupplier()}.
 * Implementers that don't override the method (the common case for plugins that don't own a
 * native allocator) must observe a {@code null} return so {@code Node.java}'s discovery loop
 * filters them out via {@code Objects::nonNull}.
 */
public class ArrowAllocatorPluginTests extends OpenSearchTestCase {

    public void testDefaultSupplierIsNull() {
        ArrowAllocatorPlugin plugin = new ArrowAllocatorPlugin() {
        };
        assertNull("default getNativeAllocatorStatsSupplier() must return null", plugin.getNativeAllocatorStatsSupplier());
    }
}
