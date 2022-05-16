/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;

import java.io.IOException;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    private final Directory storeDirectory;
    private final Directory remoteDirectory;

    public RemoteStoreRefreshListener(Directory storeDirectory, Directory remoteDirectory) {
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // ToDo Add implementation
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // ToDo Add implementation
    }
}
