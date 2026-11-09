/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests that the native jemalloc decay setters work at runtime.
 */
public class NativeAllocatorConfigTests extends OpenSearchTestCase {

    public void testSetDirtyDecayMsSucceeds() {
        // Should not throw — applies to all jemalloc arenas
        NativeAllocatorConfig.setDirtyDecayMs(5000);
        // Restore default
        NativeAllocatorConfig.setDirtyDecayMs(30000);
    }

    public void testSetMuzzyDecayMsSucceeds() {
        NativeAllocatorConfig.setMuzzyDecayMs(10000);
        NativeAllocatorConfig.setMuzzyDecayMs(30000);
    }

    public void testDisableDecayWithNegativeOne() {
        // -1 disables decay (pages retained indefinitely)
        NativeAllocatorConfig.setDirtyDecayMs(-1);
        NativeAllocatorConfig.setDirtyDecayMs(30000);
    }
}
