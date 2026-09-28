/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Transport plumbing for {@code GET _plugins/_analytics/stats}: a
 * {@link org.opensearch.action.support.nodes.TransportNodesAction} that fans
 * out from the coordinator to each cluster node, collects the local
 * {@link org.opensearch.analytics.stats.AnalyticsStats} snapshot, and
 * renders one entry per node — mirroring {@code _nodes/stats}'s shape.
 */
package org.opensearch.analytics.stats.transport;
