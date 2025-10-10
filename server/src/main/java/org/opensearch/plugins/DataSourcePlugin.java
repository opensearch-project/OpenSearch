/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface DataSourcePlugin {
    default Optional<Map<org.opensearch.vectorized.execution.search.DataFormat, DataSourceCodec>> getDataSourceCodecs() {
        return Optional.empty();
    }

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath);

    public FormatStoreDirectory<?> createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException;

    public BlobContainer createBlobContainer(BlobStore blobStore, BlobPath blobPath) throws IOException;

    DataFormat getDataFormat();
}
