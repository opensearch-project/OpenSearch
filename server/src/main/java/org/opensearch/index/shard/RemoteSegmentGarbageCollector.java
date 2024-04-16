/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

public interface RemoteSegmentGarbageCollector {

    interface Listener {

        /**
         * Called after the successful deletion of stale segments from remote store
         */
        void onSuccess(Void response);

        /**
         * Called on exception while deleting stale segments from remote store
         */
        void onFailure(Exception e);

        /**
         * Called on skipping the garbage collection
         */
        void onSkip(String reason);
    }

    Listener DEFAULT_NOOP_LISTENER = new Listener() {
        @Override
        public void onSuccess(Void response) {}

        @Override
        public void onFailure(Exception e) {}

        @Override
        public void onSkip(String reason) {}
    };

    /**
     * Deletes stale segments from remote store and notifies the listener.
     */
    void deleteStaleSegments(Listener listener);
}
