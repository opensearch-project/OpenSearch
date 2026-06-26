/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Arrow memory management for the Parquet plugin.
 *
 * <p>This package provides a managed wrapper around the unified native allocator's ingest pool
 * (see {@link org.opensearch.arrow.allocator.ArrowNativeAllocator}). The pool's dynamic limit is the
 * enforced constraint, and this package provides child allocators for individual VSR instances.
 *
 * @see org.opensearch.parquet.memory.ArrowBufferPool
 */
package org.opensearch.parquet.memory;
