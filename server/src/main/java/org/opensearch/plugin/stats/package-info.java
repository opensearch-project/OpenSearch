/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * SPI stats types for the Mustang Stats Framework.
 *
 * <p>This package contains the stats interfaces shared between
 * the OpenSearch server and native backend plugins. Types here are visible to
 * both sides without requiring a plugin dependency.
 *
 * <p>Key types:
 * <ul>
 *   <li>{@link org.opensearch.plugin.stats.PluginStats} — marker interface for all backend stats</li>
 *   <li>{@link org.opensearch.plugin.stats.BackendStatsProvider} — interface for backends to provide stats</li>
 * </ul>
 */
package org.opensearch.plugin.stats;
