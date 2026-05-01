/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
/**
 * Service Provider Interface for native-repository plugins that want to
 * integrate with a node-level block cache.
 *
 * <p>Contains {@link org.opensearch.blockcache.spi.BlockCacheProvider} (the SPI
 * interface implementing plugins register through {@code META-INF/services}),
 * {@link org.opensearch.blockcache.spi.BlockCacheKey} (namespaced cache key
 * record), and {@link org.opensearch.blockcache.spi.BlockCacheStats}
 * (point-in-time counter snapshot record).
 */
package org.opensearch.blockcache.spi;
