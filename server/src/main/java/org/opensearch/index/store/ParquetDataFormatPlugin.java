/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Set;
//
///**
// * DataFormatPlugin implementation for Parquet format support.
// * Provides Parquet-specific store directory creation and file extension handling.
// */
//public class ParquetDataFormatPlugin implements DataFormatPlugin<ParquetDataFormatPlugin.ParquetDataFormat> {
//
//    /**
//     * Singleton instance for the Parquet data format plugin
//     */
//    public static final ParquetDataFormatPlugin INSTANCE = new ParquetDataFormatPlugin();
//
//    /**
//     * Set of file extensions that Parquet format handles
//     */
//    private static final Set<String> PARQUET_EXTENSIONS = Set.of(".parquet", ".pqt");
//
//    /**
//     * Private constructor to enforce singleton pattern
//     */
//    private ParquetDataFormatPlugin() {
//    }
//
//    @Override
//    public IndexingExecutionEngine<ParquetDataFormat> indexingEngine() {
//        // For now, return null as this is not implemented yet
//        // This will be implemented when the indexing engine is needed
//        return null;
//    }
//
//    @Override
//    public DataFormat getDataFormat() {
//        return new ParquetDataFormat();
//    }
//
//    @Override
//    public FormatStoreDirectory createFormatStoreDirectory(
//        IndexSettings indexSettings,
//        ShardPath shardPath
//    ) throws IOException {
//        // Create a GenericStoreDirectory for the parquet subdirectory
//        Logger logger = LogManager.getLogger("index.store.parquet." + shardPath.getShardId());
//
//        return new GenericStoreDirectory(
//            new ParquetDataFormat(),
//            shardPath.getDataPath(),
//            PARQUET_EXTENSIONS,
//            logger
//        );
//    }
//
//    @Override
//    public FormatRemoteDirectory createFormatRemoteDirectory(
//        IndexSettings indexSettings,
//        BlobContainer baseBlobContainer,
//        String remoteBasePath
//    ) throws IOException {
//        // Create GenericRemoteDirectory for Parquet format
//        ParquetDataFormat parquetFormat = new ParquetDataFormat();
//        Logger logger = LogManager.getLogger("index.store.remote.parquet");
//        return new GenericRemoteDirectory(
//            parquetFormat,
//            remoteBasePath,
//            baseBlobContainer,
//            PARQUET_EXTENSIONS,
//            logger
//        );
//    }
//
//    @Override
//    public FormatRemoteDirectory createFormatRemoteDirectory(
//        IndexSettings indexSettings,
//        RemoteDirectory remoteDirectory,
//        String remoteBasePath
//    ) throws IOException {
//        // For Parquet format, we use BlobContainer-based approach
//        // This method should not be called for Parquet format
//        throw new UnsupportedOperationException(
//            "Parquet format uses BlobContainer-based remote storage. Use createFormatRemoteDirectory(IndexSettings, BlobContainer, String) instead."
//        );
//    }
//
//    @Override
//    public boolean supportsRemoteStorage() {
//        return true;
//    }
//
//    @Override
//    public String getRemotePathSuffix() {
//        return "parquet";
//    }
//
//    @Override
//    public Set<String> getSupportedExtensions() {
//        return PARQUET_EXTENSIONS;
//    }
//
//    /**
//     * Parquet DataFormat implementation
//     */
//    public static class ParquetDataFormat implements DataFormat {
//        @Override
//        public Setting<Settings> dataFormatSettings() {
//            return null;
//        }
//
//        @Override
//        public Setting<Settings> clusterLeveldataFormatSettings() {
//            return null;
//        }
//
//        @Override
//        public String name() {
//            return "parquet";
//        }
//
//        @Override
//        public void configureStore() {
//
//        }
//
//        @Override
//        public String getDirectoryName() {
//            return "parquet";
//        }
//    }
//}
