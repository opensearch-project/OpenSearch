/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

/**
 * Plugin extension point that supplies a {@link NativeMemoryService}.
 * <p>
 * Plugins or modules that embed a native runtime (e.g. the native-bridge module)
 * implement this interface so the core node can discover and query native memory
 * stats without a hard dependency on the JDK 25+ {@code dataformat-native} library.
 * <p>
 * This indirection exists because {@code dataformat-native} targets JDK 25 (for FFM APIs)
 * while {@code server} targets JDK 21. The {@code filterPlugins()} discovery mechanism
 * bridges the version gap at runtime.
 *
 * @opensearch.internal
 */
public interface NativeMemoryServiceProvider {

    /**
     * Returns the {@link NativeMemoryService} provided by this plugin/module,
     * or {@code null} if native memory tracking is not available.
     */
    NativeMemoryService getNativeMemoryService();
}
