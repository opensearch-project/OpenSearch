/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.plugins.Plugin;

/**
 * OpenSearch plugin entry point. The actual analytics backend logic
 * is in {@link DataFusionAnalyticsBackend} which is discovered via SPI.
 */
public class DataFusionPlugin extends Plugin {
    public DataFusionPlugin() {}
}
