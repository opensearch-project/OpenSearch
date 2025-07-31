/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assume;
import org.opensearch.datafusion.core.SessionContext;

import static org.junit.Assert.*;

/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class DataFusionServiceTest {

    private DataFusionService service;

    @Before
    public void setUp() {
        service = new DataFusionService();
        service.doStart();
    }

    @Test
    public void testGetVersion() {
        String version = service.getVersion();
        assertNotNull(version);
        assertTrue(version.contains("datafusion_version"));
        assertTrue(version.contains("arrow_version"));
    }

    @Test
    public void testCreateAndCloseContext() {
        // Create context
        long contextId = service.createContext();
        assertTrue(contextId > 0);

        // Verify context exists
        SessionContext context = service.getContext(contextId);
        assertNotNull(context);

        // Close context
        boolean closed = service.closeContext(contextId);
        assertTrue(closed);

        // Verify context is gone
        assertNull(service.getContext(contextId));
    }
}
