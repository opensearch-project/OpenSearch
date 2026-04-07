/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

/**
 * How a shuffle exchange physically moves data between data nodes.
 * Only applies to HASH/RANGE distributions. SINGLETON exchanges
 * use Analytics Core's transport (not a backend concern).
 *
 * <p>Chosen by the planner based on backend's
 * {@link org.opensearch.analytics.spi.ShuffleCapability}.
 *
 * @opensearch.internal
 */
public enum ShuffleImpl {
    /** Write Arrow IPC files locally, shuffle manifest returned to coordinator. */
    FILE,
    /** Stream partitioned data directly to target nodes. Future enhancement. */
    STREAM
}
