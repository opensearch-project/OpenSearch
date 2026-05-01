/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
/**
 * Pure-Java contract for a node-level block cache.
 *
 * <p>Contains {@link org.opensearch.blockcache.BlockCache} (the lifecycle
 * interface) and {@link org.opensearch.blockcache.BlockCacheHandle} (a stable,
 * named dependency-injection wrapper). Concrete implementations (e.g. the
 * Foyer-backed implementation in the {@code block-cache-foyer} plugin) and the
 * SPI for repository-level integration
 * ({@link org.opensearch.blockcache.spi.BlockCacheProvider}) live in the
 * {@code spi} sub-package and in sibling sandbox plugins.
 */
package org.opensearch.blockcache;
