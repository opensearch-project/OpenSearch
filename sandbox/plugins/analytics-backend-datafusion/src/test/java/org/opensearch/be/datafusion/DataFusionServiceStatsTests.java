/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link DataFusionService#getStats()}.
 *
 * Validates: Requirements 5.2, 5.3, 5.5
 *
 * Note: Cache TTL behavior (Requirement 5.3 — same instance within TTL window,
 * fresh instance after TTL expires) requires a running native runtime since
 * {@code doStart()} calls {@code NativeBridge.stats()} to seed the cache.
 * That behavior is verified in integration tests where the native library is loaded.
 */
public class DataFusionServiceStatsTests extends OpenSearchTestCase {

    /**
     * Validates Requirement 5.5: getStats() throws IllegalStateException before doStart().
     *
     * When the service is constructed but not started, the statsCache field is null.
     * Calling getStats() must throw IllegalStateException with a descriptive message.
     */
    public void testGetStatsBeforeStartThrowsIllegalStateException() {
        DataFusionService service = DataFusionService.builder().build();

        IllegalStateException ex = expectThrows(IllegalStateException.class, service::getStats);
        assertEquals("DataFusionService has not been started", ex.getMessage());
    }
}
