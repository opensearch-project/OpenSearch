/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;

public interface DataFormatPlugin  {

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath);

    DataFormat getDataFormat();

    /**
     * Creates a format-specific store directory
     * @param indexSettings the index settings
     * @param shardPath the shard path where directories should be created
     * @return FormatStoreDirectory instance for this format
     */
     FormatStoreDirectory createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    )throws IOException;

}
