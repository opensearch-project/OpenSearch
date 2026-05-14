/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * SPI types for native memory services.
 *
 * <p>This package contains the service provider interfaces that allow the OpenSearch
 * server to query native (jemalloc) memory statistics from sandbox modules without
 * a compile-time dependency on JDK 25+ FFM APIs.
 *
 * <p>Key types:
 * <ul>
 *   <li>{@link org.opensearch.nativebridge.spi.NativeStatsProvider} — plugin extension point for native stats discovery</li>
 *   <li>{@link org.opensearch.nativebridge.spi.NativeMemoryStats} — immutable stats POJO</li>
 * </ul>
 */
package org.opensearch.nativebridge.spi;
