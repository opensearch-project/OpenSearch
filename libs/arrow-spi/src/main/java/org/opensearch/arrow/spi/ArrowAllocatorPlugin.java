/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

/**
 * Marker plugin SPI for components that own a {@link NativeAllocator}.
 *
 * <p>Implemented by the plugin that constructs and owns the node-level Arrow
 * allocator (today: {@code ArrowBasePlugin}). The server-side {@code NodeService}
 * locates the allocator through this interface to render allocator stats under
 * {@code _nodes/stats/native_allocator}, without taking a hard dependency on the
 * Arrow plugin itself — the SPI lives in {@code arrow-spi}, which {@code server}
 * already depends on.
 *
 * <p>Mirrors the lookup pattern used for other plugin-provided node-level
 * resources (e.g. {@code SearchBackEndPlugin#getAnalyticsBackendNativeMemoryStats}).
 *
 * @opensearch.api
 */
public interface ArrowAllocatorPlugin {

    /**
     * Returns the node-level native allocator owned by this plugin, or {@code null}
     * if the allocator has not yet been initialized (e.g. the plugin's
     * {@code createComponents} has not run).
     */
    NativeAllocator getNativeAllocator();
}
