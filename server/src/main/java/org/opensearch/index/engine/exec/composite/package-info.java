/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Composite data format implementations that enable multi-format indexing within OpenSearch.
 * 
 * <p>This package provides the infrastructure for handling multiple data formats simultaneously
 * within a single indexing operation. The composite approach allows OpenSearch to write data
 * in different formats concurrently, enabling optimized storage and retrieval patterns for
 * different use cases.
 * 
 * <p>Key components include:
 * <ul>
 * <li>{@link CompositeDataFormatWriter} - Orchestrates writes across multiple data format writers</li>
 * <li>{@link CompositeDocumentInput} - Aggregates document inputs for multiple formats</li>
 * <li>{@link CompositeIndexingExecutionEngine} - Manages execution across composite formats</li>
 * </ul>
 * 
 * <p>This design enables scenarios such as:
 * <ul>
 * <li>Writing to both columnar and row-based formats simultaneously</li>
 * <li>Maintaining multiple indexes with different optimization characteristics</li>
 * <li>Supporting hybrid storage architectures</li>
 * </ul>
 */
package org.opensearch.index.engine.exec.composite;