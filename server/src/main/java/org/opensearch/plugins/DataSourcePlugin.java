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
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FieldAssignments;
import org.opensearch.index.engine.exec.FieldSupportRegistry;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.spi.vectorized.DataSourceCodec;
import org.opensearch.index.store.FormatStoreDirectory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface DataSourcePlugin {
    default Optional<Map<org.opensearch.plugins.spi.vectorized.DataFormat, DataSourceCodec>> getDataSourceCodecs() {
        return Optional.empty();
    }

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(EngineConfig engineConfig, MapperService mapperService, boolean isPrimary, ShardPath shardPath, IndexSettings indexSettings, FieldAssignments fieldAssignments);

    FormatStoreDirectory<?> createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException;

    BlobContainer createBlobContainer(BlobStore blobStore, BlobPath blobPath) throws IOException;

    DataFormat getDataFormat();

    /**
     * Registers the field type capabilities this plugin's data format supports.
     * Plugins override this to declare which field types their format can handle
     * and with what capabilities (STORE, INDEX, DOC_VALUES).
     */
    default void registerFieldSupport(FieldSupportRegistry registry) {
        // Default no-op; plugins override to register their field type capabilities
    }
}
