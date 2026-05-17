/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import org.opensearch.test.OpenSearchTestCase;

public class NativeAllocatorPoolConfigTests extends OpenSearchTestCase {

    public void testPoolConstants() {
        assertEquals("flight", NativeAllocatorPoolConfig.POOL_FLIGHT);
        assertEquals("ingest", NativeAllocatorPoolConfig.POOL_INGEST);
    }

    public void testSettingKeys() {
        assertEquals("native.allocator.pool.flight.min", NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN);
        assertEquals("native.allocator.pool.flight.max", NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX);
        assertEquals("native.allocator.pool.ingest.min", NativeAllocatorPoolConfig.SETTING_INGEST_MIN);
        assertEquals("native.allocator.pool.ingest.max", NativeAllocatorPoolConfig.SETTING_INGEST_MAX);
    }

    public void testRootSettingKey() {
        assertEquals("native.allocator.root.limit", NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT);
    }
}
