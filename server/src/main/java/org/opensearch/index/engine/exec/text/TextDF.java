/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.text;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.index.store.GenericStoreDirectory;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;


public class TextDF extends Plugin implements DataFormat, DataSourcePlugin {
    @Override
    public Setting<Settings> dataFormatSettings() {
        return null;
    }

    @Override
    public Setting<Settings> clusterLeveldataFormatSettings() {
        return null;
    }

    @Override
    public String name() {
        return "text";
    }

    @Override
    public void configureStore() {

    }

    @Override
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings) {
        return  (IndexingExecutionEngine<T>) new TextEngine();
    }

    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        return new GenericStoreDirectory<>(
            new TextDF(),
            shardPath
        );
    }

    @Override
    public BlobContainer createBlobContainer(BlobStore blobStore, BlobPath blobPath) throws IOException {
        BlobPath formatPath = blobPath.add(getDataFormat().name().toLowerCase());
        return blobStore.blobContainer(formatPath);
    }

    @Override
    public DataFormat getDataFormat() {
        return new TextDF();
    }
}
