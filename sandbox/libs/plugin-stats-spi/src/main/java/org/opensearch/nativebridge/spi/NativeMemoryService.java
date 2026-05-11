/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

/**
 * Service interface for retrieving native (jemalloc) memory statistics
 * from the Rust layer via FFM.
 * <p>
 * The concrete implementation ({@code FfmNativeMemoryService} in {@code libs/dataformat-native})
 * is discovered at runtime via {@link NativeMemoryServiceProvider} and {@code filterPlugins()}.
 * This indirection exists because {@code dataformat-native} targets JDK 25 (for stable FFM APIs)
 * while {@code server} targets JDK 21.
 *
 * @opensearch.internal
 */
public interface NativeMemoryService {

    /**
     * Returns a snapshot of native memory statistics, or {@code null} if
     * stats are unavailable.
     *
     * @return current native memory stats
     */
    NativeMemoryStats stats();
}
