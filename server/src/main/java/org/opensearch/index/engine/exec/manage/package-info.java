/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Engine management implementations that orchestrate the lifecycle and operations of indexing engines.
 * 
 * <p>This package contains the high-level management layer responsible for coordinating
 * multiple indexing engines, managing their lifecycle, and providing unified access to
 * their capabilities. It serves as the control plane for the engine execution framework.
 * 
 * <p>Key components include:
 * <ul>
 * <li>{@link ComposableEngine} - Main engine implementation that composes multiple data formats</li>
 * <li>{@link CatalogSnapshot} - Immutable snapshot of searchable data files and metadata</li>
 * <li>{@link ReleasableRef} - Reference counting mechanism for resource management</li>
 * </ul>
 * 
 * <p>The management layer handles:
 * <ul>
 * <li>Engine initialization and configuration</li>
 * <li>Refresh operations and snapshot management</li>
 * <li>Resource lifecycle and cleanup</li>
 * <li>Coordination between different storage formats</li>
 * <li>Thread-safe access to engine resources</li>
 * </ul>
 * 
 * <p>This design enables safe concurrent access to engine resources while maintaining
 * consistency across multiple data formats and providing efficient resource utilization.
 */
package org.opensearch.index.engine.exec.manage;