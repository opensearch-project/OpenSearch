/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * OpenSearch plugin that provides subdirectory-aware store functionality.
 *
 * This package contains classes that enable OpenSearch stores to handle files
 * organized in subdirectories within shard data paths, with support for
 * peer recovery operations.
 *
 * <h2>Key Components:</h2>
 * <ul>
 *   <li>{@link org.opensearch.plugin.store.subdirectory.SubdirectoryStorePlugin} - Main plugin class</li>
 *   <li>{@link org.opensearch.plugin.store.subdirectory.SubdirectoryAwareStore} - Store implementation</li>
 * </ul>
 *
 * <h2>Usage:</h2>
 * <p>
 * Configure an index to use the subdirectory store by setting:
 * </p>
 * <pre>
 * {
 *   "index.store.factory": "subdirectory_store"
 * }
 * </pre>
 */
package org.opensearch.plugin.store.subdirectory;
