/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.parquet.ParquetSettings;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Per-engine PME (Parquet Modular Encryption) context.
 *
 * <p>This is the public entry point for callers. Each {@code ParquetIndexingEngine}
 * holds one instance (or {@code null} for unencrypted indices). The context ties
 * together:
 * <ul>
 *   <li>The index-level {@link CryptoMetadata} (key provider type and name).</li>
 *   <li>The index UUID and shard ID, which together form the cache key in
 *       {@link PmeDataKeyCache} and mirror the shard-lifecycle eviction model of the
 *       Lucene storage-encryption plugin.</li>
 *   <li>The index data path, which is the directory that holds the {@code keyfile}.</li>
 * </ul>
 *
 * <p>The data key itself lives in the node-level {@link PmeDataKeyCache}; this class
 * never holds raw key bytes directly.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #create(IndexSettings, Path, int)} — called once per engine at shard open time.</li>
 *   <li>{@link #createFileEncryptionInputs()} — called once per Parquet file to be written.</li>
 *   <li>{@link #evict()} — called when the engine (shard) is closed; zeroes this shard's
 *       cached key material.</li>
 * </ol>
 */
public final class PmeContext {

    private static final Logger logger = LogManager.getLogger(PmeContext.class);

    private final CryptoMetadata cryptoMetadata;
    private final Path indexDataPath;
    private final String indexUuid;
    private final int shardId;

    private PmeContext(CryptoMetadata cryptoMetadata, Path indexDataPath, String indexUuid, int shardId) {
        this.cryptoMetadata = cryptoMetadata;
        this.indexDataPath = indexDataPath;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
    }

    /**
     * Creates a {@link PmeContext} if the index is configured for PME encryption, or
     * returns {@code null} for unencrypted indices.
     *
     * <p>On first call for a given shard, the keyfile is created via
     * {@link PmeKeyfileManager#initOrLoad} and the data key is loaded into
     * {@link PmeDataKeyCache}. Subsequent calls (from other shards of the same index)
     * hit the cache under their own shard-keyed entry.
     *
     * @param indexSettings  the index settings; {@code null} → returns {@code null}
     * @param indexDataPath  index-level data directory (parent of shard directories)
     * @param shardId        the shard ID; used as part of the cache key for shard-lifecycle eviction
     * @return context, or {@code null} if encryption is not configured
     * @throws IOException if keyfile initialisation or key loading fails
     */
    public static PmeContext create(IndexSettings indexSettings, Path indexDataPath, int shardId) throws IOException {
        return create(indexSettings, indexDataPath, shardId, null);
    }

    /**
     * Exposed for testing only:
     *
     * <p>The {@code loaderOverride} parameter is package-private for testing: pass a
     * pre-built {@link PmeDataKeyCache.DataKeyLoader} (backed by a mock
     * {@link org.opensearch.common.crypto.MasterKeyProvider}) to bypass
     * {@link org.opensearch.crypto.CryptoHandlerRegistry}. Pass {@code null} for
     * production use; the loader will then be derived from {@code indexSettings}.
     *
     * @param indexSettings  the index settings; {@code null} → returns {@code null}
     * @param indexDataPath  index-level data directory (parent of shard directories)
     * @param shardId        the shard ID; used as part of the cache key for shard-lifecycle eviction
     * @param loaderOverride test-only key loader; {@code null} in production
     * @return context, or {@code null} if encryption is not configured
     * @throws IOException if keyfile initialisation or key loading fails
     */
    static PmeContext create(IndexSettings indexSettings, Path indexDataPath, int shardId, PmeDataKeyCache.DataKeyLoader loaderOverride)
        throws IOException {
        if (indexSettings == null) {
            return null;
        }
        // Build CryptoMetadata from Parquet-specific settings (index.store.parquet.crypto.*)
        // instead of calling CryptoMetadata.fromIndexSettings(), which reads the Lucene-plugin
        // keys (index.store.crypto.*) that we cannot register to avoid a setting-name collision.
        Settings raw = indexSettings.getSettings();
        String keyProviderName = ParquetSettings.CRYPTO_KEY_PROVIDER.get(raw);
        if (keyProviderName == null || keyProviderName.isEmpty()) {
            return null;
        }
        String keyProviderType = ParquetSettings.CRYPTO_KEY_PROVIDER_TYPE.get(raw);
        CryptoMetadata cryptoMetadata = new CryptoMetadata(keyProviderName, keyProviderType, Settings.EMPTY);
        String indexUuid = indexSettings.getUUID();
        PmeContext ctx = new PmeContext(cryptoMetadata, indexDataPath, indexUuid, shardId);
        PmeDataKeyCache.DataKeyLoader loader = loaderOverride != null
            ? loaderOverride
            : () -> PmeKeyfileManager.initOrLoad(indexDataPath, cryptoMetadata);
        // Eagerly populate the cache so errors surface at shard-open time, not at write time.
        PmeDataKeyCache.getInstance().getOrLoad(indexUuid, shardId, PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, loader);
        logger.debug("PME context initialised for index [{}] shard [{}]", indexUuid, shardId);
        return ctx;
    }


    /**
     * Creates per-file PME encryption inputs for a new Parquet file.
     *
     * <p>Retrieves the data key from {@link PmeDataKeyCache} (cache hit in the normal case),
     * then delegates to {@link PmeFileEncryptionInputs#create(PmeDataKey)}.
     * The returned inputs hold derived key material; the caller (
     * {@code NativeParquetWriter}) is responsible for calling
     * {@link PmeFileEncryptionInputs#zero()} immediately after the native writer
     * has consumed them.
     *
     * @return single-use per-file encryption inputs
     * @throws IOException if the data key cannot be loaded
     */
    public PmeFileEncryptionInputs createFileEncryptionInputs() throws IOException {
        logger.trace("PME: createFileEncryptionInputs for index=[{}] shard=[{}]", indexUuid, shardId);
        PmeDataKey dataKey = PmeDataKeyCache.getInstance().getOrLoad(
            indexUuid,
            shardId,
            PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID,
            () -> PmeKeyfileManager.initOrLoad(indexDataPath, cryptoMetadata)
        );
        PmeFileEncryptionInputs inputs = PmeFileEncryptionInputs.create(dataKey);
        logger.trace("PME: file encryption inputs created for index=[{}] shard=[{}]", indexUuid, shardId);
        return inputs;
    }

    /**
     * Evicts this shard's data key from the node-level cache, zeroing key material.
     * Must be called when the engine is closed (i.e. from {@code ParquetIndexingEngine#close}).
     * Other shards of the same index that are still open retain their own cache entries.
     */
    public void evict() {
        PmeDataKeyCache.getInstance().evict(indexUuid, shardId, PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID);
        logger.debug("PME data key evicted for index [{}] shard [{}]", indexUuid, shardId);
    }
}
