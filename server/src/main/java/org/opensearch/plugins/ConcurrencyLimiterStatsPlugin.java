/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.action.ActionConcurrencyLimiterStats;

/**
 * Plugin interface implemented by modules that provide adaptive per-action concurrency limiting.
 * <p>
 * Implementing this interface allows the module to expose live limiter statistics
 * through the standard {@code /_nodes/stats} API without creating a hard dependency
 * from the server module on the concurrency-limit module.
 */
public interface ConcurrencyLimiterStatsPlugin {

    /**
     * Returns a snapshot of all configured limiters, or {@code null} if the registry
     * has not been initialised yet (e.g. the node is still starting up).
     */
    ActionConcurrencyLimiterStats getConcurrencyLimiterStats();
}
