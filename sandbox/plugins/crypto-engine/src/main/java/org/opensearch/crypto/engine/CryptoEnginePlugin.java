/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.engine;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.ShardCryptoContext;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Layer 2 Storage Encryption Orchestrator plugin ("cryptofs").
 * Unwraps encryption key metadata into format-agnostic {@link ShardCryptoContext}
 * and decorates storage handles transparently across all data formats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CryptoEnginePlugin extends Plugin implements IndexStorePlugin {

    public static final String STORE_TYPE = "cryptofs";

    private final CryptoStoreDecorator decorator = new CryptoStoreDecorator();

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(STORE_TYPE, new DirectoryFactory() {
            @Override
            public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
                Path location = shardPath.resolveIndex();
                Directory rawDirectory = FSDirectory.open(location);
                ShardCryptoContext cryptoContext = CryptoKeyUnwrapper.buildShardCryptoContext(indexSettings, null);
                return decorator.decorate(rawDirectory, cryptoContext);
            }

            @Override
            public Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
                Directory rawDirectory = FSDirectory.open(location, lockFactory);
                ShardCryptoContext cryptoContext = CryptoKeyUnwrapper.buildShardCryptoContext(indexSettings, null);
                return decorator.decorate(rawDirectory, cryptoContext);
            }
        });
    }
}
