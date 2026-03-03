/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.backend.EngineCapabilities;
import org.opensearch.plugins.Plugin;

/**
 * DataFusionPlugin native execution engine plugin.
 */
public class DataFusionPlugin extends Plugin implements AnalyticsBackEndPlugin {

    private final DataFusionBridge bridge = new DataFusionBridge();

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineBridge<?> bridge() {
        return bridge;
    }

    @Override
    public EngineCapabilities getEngineCapabilities() {
        return null;
    }
}
