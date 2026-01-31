/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene-specific bridge implementations for the OpenSearch engine execution framework.
 * 
 * <p>This package contains concrete implementations of bridge interfaces that integrate
 * with Apache Lucene components. These implementations provide the necessary adapters
 * to connect the engine execution framework with Lucene's indexing and search capabilities.
 * 
 * <p>Key components include:
 * <ul>
 * <li>Configuration providers that wrap Lucene engine configurations</li>
 * <li>Commit data implementations that expose Lucene index commit information</li>
 * <li>Utility classes for Lucene-specific operations</li>
 * </ul>
 */
package org.opensearch.index.engine.exec.bridge.lucene;