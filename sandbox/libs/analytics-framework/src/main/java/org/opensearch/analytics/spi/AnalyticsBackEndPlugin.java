/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.backend.EngineCapabilities;

/**
 * SPI extension point for back-end execution engines (DataFusion, Lucene, etc.).
 *
 * <p>Implementations are discovered by the analytics engine hub via
 * {@code ExtensiblePlugin.loadExtensions} and passed to the
 * {@link QueryPlanExecutorPlugin} during initialization.
 *
 * @opensearch.internal
 */
public interface AnalyticsBackEndPlugin {
    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /** JNI boundary for executing serialized plans, or null for engines without native execution. */
    EngineBridge<?> bridge();

    /** Engine capabilities describing supported operators/functions, or null. */
    EngineCapabilities getEngineCapabilities();
}
