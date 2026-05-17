/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Stats types for the analytics backend plugin layer.
 *
 * <p>This package contains stats classes shared between
 * the OpenSearch server and native backend plugins. Types here are visible to
 * both sides without requiring a plugin dependency.
 *
 * <p>Key types:
 * <ul>
 *   <li>{@link org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats} — task cancellation counters from the analytics backend</li>
 * </ul>
 */
package org.opensearch.plugin.stats;
