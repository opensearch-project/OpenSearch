/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.settings.Settings;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class DataFusionServiceTest extends OpenSearchTestCase {

    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings mockSettings = Settings.builder().put("path.data", "/tmp/test-data").build();

        when(mockEnvironment.settings()).thenReturn(mockSettings);
        service = new DataFusionService(mockEnvironment);
        service.doStart();
    }

    public void testGetVersion() {
        String version = service.getVersion();
        assertNotNull(version);
        assertTrue(version.contains("datafusion_version"));
        assertTrue(version.contains("substrait_version"));
    }

    public void testCreateAndCloseContext() {
        // Create context
        SessionContext defaultContext = service.getDefaultContext();
        assertNotNull(defaultContext);
        assertTrue(defaultContext.getContext() > 0);

        // Verify context exists
        SessionContext context = service.getContext(defaultContext.getContext());
        assertNotNull(context);
        assertEquals(defaultContext.getContext(), context.getContext());

        // Close context
        boolean closed = service.closeContext(defaultContext.getContext());
        assertTrue(closed);

        // Verify context is gone
        assertNull(service.getContext(defaultContext.getContext()));
    }
}
