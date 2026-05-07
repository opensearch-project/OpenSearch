/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Node-level Arrow memory management. Plugins that produce or consume Arrow data
 * obtain child allocators from {@link org.opensearch.arrow.memory.ArrowAllocatorService},
 * which roots them under a single per-node {@link org.apache.arrow.memory.RootAllocator}
 * so cross-plugin buffer handoffs pass Arrow's {@code AllocationManager.associate} check.
 */
package org.opensearch.arrow.memory;
