/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * SPI extension point for query language front-ends (PPL, SQL, etc.).
 *
 * <p>Marker interface for discovery by the analytics engine hub.
 * Front-ends receive {@code QueryPlanExecutor} and {@code SchemaProvider}
 * via Guice injection.
 *
 * @opensearch.internal
 */
public interface AnalyticsFrontEndPlugin {
    // Marker interface for SPI discovery
}
