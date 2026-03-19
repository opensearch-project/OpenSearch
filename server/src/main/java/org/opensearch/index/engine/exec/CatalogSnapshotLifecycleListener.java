/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;

/**
 * Unified lifecycle listener for catalog snapshots.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CatalogSnapshotLifecycleListener {

    CatalogSnapshotLifecycleListener NOOP = new CatalogSnapshotLifecycleListener() {
        @Override
        public void beforeRefresh() {}

        @Override
        public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) {}

        @Override
        public void onDeleted(CatalogSnapshot catalogSnapshot) {}
    };

    void beforeRefresh() throws IOException;

    void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException;

    void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException;
}
