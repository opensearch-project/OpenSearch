/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Bridge interfaces that connect the OpenSearch engine execution framework with underlying storage implementations.
 * 
 * <p>This package defines the core abstraction layer that allows the engine execution framework
 * to work with different storage backends and indexing technologies. The bridge pattern is used
 * to decouple the high-level engine operations from specific implementation details.
 * 
 * <p>Key interfaces include:
 * <ul>
 * <li>{@link ConfigurationProvider} - Provides configuration settings for indexing operations</li>
 * <li>{@link OperationMapper} - Maps high-level operations to storage-specific implementations</li>
 * <li>{@link CommitData} - Provides access to commit metadata and user data</li>
 * <li>{@link Indexer} - Core indexing interface for document operations</li>
 * </ul>
 * 
 * <p>These interfaces enable pluggable storage backends while maintaining a consistent
 * API for the engine execution layer.
 */
package org.opensearch.index.engine.exec.bridge;