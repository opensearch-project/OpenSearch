/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.engine;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.crypto.ShardCryptoContext;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolves master key providers and unwraps encryption keys from index metadata
 * into a format-agnostic {@link ShardCryptoContext} (Layer 2 orchestrator).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CryptoKeyUnwrapper {

    /**
     * Constructs a {@link ShardCryptoContext} for a shard based on the index settings and optional master key provider.
     *
     * @param indexSettings the index settings containing crypto provider configuration
     * @param masterKeyProvider optional master key provider instance
     * @return populated ShardCryptoContext, or null if storage encryption is disabled for the index
     */
    public static ShardCryptoContext buildShardCryptoContext(IndexSettings indexSettings, MasterKeyProvider masterKeyProvider) {
        Objects.requireNonNull(indexSettings, "indexSettings must not be null");
        if (!indexSettings.isStorageEncryptionEnabled()) {
            return null;
        }

        CryptoMetadata cryptoMetadata = CryptoMetadata.fromIndexSettings(indexSettings.getSettings());
        String providerName = indexSettings.getCryptoKeyProvider();
        String providerType = cryptoMetadata != null ? cryptoMetadata.keyProviderType() : "default";
        String keyArn = cryptoMetadata != null ? cryptoMetadata.getKeyArn().orElse(null) : null;

        DataKeyPair dataKeyPair = null;
        Map<String, String> encContext = Map.of();

        if (masterKeyProvider != null) {
            dataKeyPair = masterKeyProvider.generateDataPair();
            encContext = masterKeyProvider.getEncryptionContext();
        }

        return new ShardCryptoContext(
            providerName,
            providerType,
            keyArn,
            dataKeyPair,
            encContext
        );
    }
}
