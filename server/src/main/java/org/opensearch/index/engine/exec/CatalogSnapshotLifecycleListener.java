/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Unified lifecycle listener for catalog snapshots.
 * <p>
 * Combines refresh notifications (create/update) and delete notifications
 * into a single interface so plugins only need to wire one listener.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CatalogSnapshotLifecycleListener {

    /** Singleton that silently ignores every callback. */
    CatalogSnapshotLifecycleListener NOOP = new CatalogSnapshotLifecycleListener() {
        @Override
        public void beforeRefresh() {}

        @Override
        public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) {}

        @Override
        public void onDeleted(CatalogSnapshot catalogSnapshot) {}
    };

    /**
     * Called before a refresh operation.
     */
    void beforeRefresh() throws IOException;

    /**
     * Called after a refresh operation with the resulting catalog snapshot.
     * @param didRefresh whether the refresh actually occurred
     * @param catalogSnapshot the current catalog snapshot with file information
     */
    void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException;

    /**
     * Called when a catalog snapshot is deleted.
     * @param catalogSnapshot the snapshot being deleted
     */
    void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException;
}
