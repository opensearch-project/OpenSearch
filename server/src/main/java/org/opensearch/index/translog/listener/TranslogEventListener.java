/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.listener;

/**
 * The listener that gets fired on events related to {@link org.opensearch.index.translog.TranslogManager}
 *
 * @opensearch.internal
 */
public interface TranslogEventListener {

    TranslogEventListener NOOP_TRANSLOG_EVENT_LISTENER = new TranslogEventListener() {
    };

    /**
     * Invoked after translog sync operations
     */
    default void onAfterTranslogSync() {}

    /**
     * Invoked after recovering operations from translog
     */
    default void onAfterTranslogRecovery() {}

    /**
     * Invoked before recovering operations from translog
     */
    default void onBeginTranslogRecovery() {}

    /**
     * Invoked when translog operations run into any other failure
     * @param reason the failure reason
     * @param ex the failure exception
     */
    default void onFailure(String reason, Exception ex) {}
}
