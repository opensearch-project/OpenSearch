/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TelemetryModuleTests extends OpenSearchTestCase {

    public void testGetTelemetrySuccess() {
        TelemetryPlugin telemetryPlugin = mock(TelemetryPlugin.class);
        when(telemetryPlugin.getName()).thenReturn("otel");
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(telemetryPlugin.getTelemetry(any())).thenReturn(Optional.of(mockTelemetry));
        List<TelemetryPlugin> telemetryPlugins = List.of(telemetryPlugin);

        TelemetryModule telemetryModule = new TelemetryModule(telemetryPlugins, any());

        assertTrue(telemetryModule.getTelemetry().isPresent());
        assertEquals(mockTelemetry, telemetryModule.getTelemetry().get());
    }

    public void testGetTelemetryWithMultipleInstalledPlugins() {
        TelemetryPlugin telemetryPlugin1 = mock(TelemetryPlugin.class);
        TelemetryPlugin telemetryPlugin2 = mock(TelemetryPlugin.class);
        when(telemetryPlugin1.getName()).thenReturn("otel");
        Telemetry mockTelemetry1 = mock(Telemetry.class);
        Telemetry mockTelemetry2 = mock(Telemetry.class);

        when(telemetryPlugin1.getTelemetry(any())).thenReturn(Optional.of(mockTelemetry1));
        when(telemetryPlugin2.getTelemetry(any())).thenReturn(Optional.of(mockTelemetry2));

        List<TelemetryPlugin> telemetryPlugins = List.of(telemetryPlugin1, telemetryPlugin2);

        try {
            TelemetryModule telemetryModule = new TelemetryModule(telemetryPlugins, any());
        } catch (Exception e) {
            assertEquals("Cannot register more than one telemetry", e.getMessage());
        }

    }

    public void testGetTelemetryWithNoPlugins() {
        TelemetryPlugin telemetryPlugin = mock(TelemetryPlugin.class);
        when(telemetryPlugin.getName()).thenReturn("otel");
        TelemetryModule telemetryModule = new TelemetryModule(List.of(telemetryPlugin), any());

        assertFalse(telemetryModule.getTelemetry().isPresent());

    }

}
