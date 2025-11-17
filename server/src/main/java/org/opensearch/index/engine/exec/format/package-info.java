/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Data format plugin interfaces that define the extensible architecture for supporting multiple storage formats.
 * 
 * <p>This package establishes the plugin framework that allows OpenSearch to support various
 * data formats beyond the traditional Lucene-based storage. The plugin architecture enables
 * third-party developers and OpenSearch itself to implement custom storage formats optimized
 * for specific use cases.
 * 
 * <p>Key interfaces include:
 * <ul>
 * <li>{@link DataSourcePlugin} - Main plugin interface for registering new data formats</li>
 * <li>{@link DataFormat} - Defines the characteristics and capabilities of a data format</li>
 * <li>{@link IndexingExecutionEngine} - Execution engine interface for format-specific operations</li>
 * </ul>
 * 
 * <p>This extensible design supports:
 * <ul>
 * <li>Columnar storage formats for analytical workloads</li>
 * <li>Time-series optimized formats for observability data</li>
 * <li>Custom compression and encoding schemes</li>
 * <li>Integration with external storage systems</li>
 * </ul>
 * 
 * <p>Plugins implementing these interfaces are discovered and loaded at runtime,
 * allowing for dynamic extension of OpenSearch's storage capabilities.
 */
package org.opensearch.index.engine.exec.format;