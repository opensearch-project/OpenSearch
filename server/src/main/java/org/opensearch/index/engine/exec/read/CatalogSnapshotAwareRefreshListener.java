/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.read;

import org.opensearch.index.engine.exec.manage.CatalogSnapshot;

import java.io.IOException;

/**
 * Base Listener interface for refresh lifecycle of catalog snapshot
 */
public interface CatalogSnapshotAwareRefreshListener {
    /**
     * Called before refresh operation.
     */
    void beforeRefresh() throws IOException;

    /**
     * Called after refresh operation with catalog snapshot.
     * @param didRefresh whether refresh actually occurred
     * @param catalogSnapshot the current catalog snapshot with file information
     */
    void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException;
}
