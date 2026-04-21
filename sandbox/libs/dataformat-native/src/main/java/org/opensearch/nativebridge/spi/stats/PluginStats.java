/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi.stats;

/**
 * Marker interface for all backend stats types in the Mustang Stats Framework.
 *
 * <p>Intentionally empty — serves as the common type for
 * {@link BackendStatsProvider#getBackendStats()}. Each backend's top-level stats
 * class (e.g. {@code DataFusionStats}) implements this interface so the Analytics
 * Plugin can discover and iterate over them.
 */
public interface PluginStats {
    // marker — no methods
}
