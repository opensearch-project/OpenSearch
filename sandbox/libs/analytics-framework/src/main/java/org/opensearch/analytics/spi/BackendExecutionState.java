/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Marker interface for backend-specific execution state that flows between
 * successive instruction handler calls. The first handler in the chain receives
 * {@code null} and bootstraps the state; subsequent handlers receive and build
 * upon the previous handler's output.
 *
 * <p>Each backend defines its own concrete implementation (e.g.,
 * {@code DataFusionSessionState} holding a native SessionContext handle).
 *
 * @opensearch.internal
 */
public interface BackendExecutionState {}
