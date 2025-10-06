/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Manages throttling for indexing. This can be used to compose different throttling signals,
 * and limit indexing on the nodes to allow to handle strenuous situations.
 */
@ExperimentalApi
public interface IndexingThrottler {

    /**
     * Returns the number of milliseconds this engine was under index throttling.
     */
    long getIndexThrottleTimeInMillis();

    /**
     * Returns the <code>true</code> iff this engine is currently under index throttling.
     * @see #getIndexThrottleTimeInMillis()
     */
    boolean isThrottled();

    /**
     * Request that this engine throttle incoming indexing requests to one thread.
     * Must be matched by a later call to {@link #deactivateThrottling()}.
     */
    void activateThrottling();

    /**
     * Reverses a previous {@link #activateThrottling} call.
     */
    void deactivateThrottling();
}
