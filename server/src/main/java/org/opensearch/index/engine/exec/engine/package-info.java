/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Core engine execution interfaces that define the fundamental operations for document indexing and data management.
 * 
 * <p>This package contains the essential interfaces and data structures that form the foundation
 * of the engine execution framework. These components define how documents are processed,
 * written to storage, and how the results of these operations are communicated throughout
 * the system.
 * 
 * <p>Key interfaces and classes include:
 * <ul>
 * <li>{@link Writer} - Core interface for writing documents to storage formats</li>
 * <li>{@link DocumentInput} - Represents a document being prepared for indexing</li>
 * <li>{@link WriteResult} - Encapsulates the outcome of write operations</li>
 * <li>{@link RefreshInput} and {@link RefreshResult} - Handle refresh operation data</li>
 * <li>{@link FileMetadata} - Metadata about files created during indexing</li>
 * <li>{@link IndexingConfiguration} - Configuration parameters for indexing operations</li>
 * </ul>
 * 
 * <p>These interfaces support:
 * <ul>
 * <li>Pluggable storage format implementations</li>
 * <li>Efficient document processing pipelines</li>
 * <li>Comprehensive error handling and reporting</li>
 * <li>Metadata tracking for file management</li>
 * <li>Configurable indexing behavior</li>
 * </ul>
 * 
 * <p>The design emphasizes type safety, extensibility, and performance while providing
 * clear contracts for implementing custom storage formats and indexing strategies.
 */
package org.opensearch.index.engine.exec.engine;