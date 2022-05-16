/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;

public class RemoteDirectoryFactory implements IndexStorePlugin.RemoteDirectoryFactory {

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path, Repository repository) throws IOException {
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        BlobPath blobPath = new BlobPath();
        blobPath = blobPath.add(indexSettings.getIndex().getName()).add(String.valueOf(path.getShardId().getId()));
        BlobContainer blobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(blobPath);
        return new RemoteDirectory(blobContainer);
    }
}
