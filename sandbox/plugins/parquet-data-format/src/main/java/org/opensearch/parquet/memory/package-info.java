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
 * <p>This package provides a managed wrapper around Apache Arrow's {@code RootAllocator},
 * with allocation limits derived from the {@code index.parquet.max_native_allocation} setting.
 * The pool computes the maximum allocation as a percentage of available non-heap system memory
 * and provides child allocators for individual VSR instances.
 *
 * @see org.opensearch.parquet.memory.ArrowBufferPool
 * @see org.opensearch.parquet.ParquetSettings#MAX_NATIVE_ALLOCATION
 */
package org.opensearch.parquet.memory;
