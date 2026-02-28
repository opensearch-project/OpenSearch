/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.lucene.engine.LuceneExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.plugins.DataSourcePlugin;

import org.opensearch.plugins.Plugin;

import java.io.IOException;

public class LuceneDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, boolean isPrimary, ShardPath shardPath, IndexSettings indexSettings) {
        return (IndexingExecutionEngine<T>) new LuceneExecutionEngine(mapperService, shardPath, indexSettings);
    }

    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        return null;
    }

    @Override
    public BlobContainer createBlobContainer(BlobStore blobStore, BlobPath blobPath) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return new LuceneDataFormat();
    }

}
