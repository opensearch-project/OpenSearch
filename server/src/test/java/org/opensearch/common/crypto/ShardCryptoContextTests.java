/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.crypto;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Optional;

public class ShardCryptoContextTests extends OpenSearchTestCase {

    public void testShardCryptoContextCreation() {
        DataKeyPair dataKeyPair = new DataKeyPair("enc_key".getBytes(), "dec_key".getBytes());
        ShardCryptoContext context = new ShardCryptoContext(
            "aws-kms",
            "kms",
            "arn:aws:kms:us-east-1:123456789012:key/test-key",
            dataKeyPair,
            Map.of("tenant", "corp")
        );

        assertEquals("aws-kms", context.getKeyProviderName());
        assertEquals("kms", context.getKeyProviderType());
        assertTrue(context.getKeyArn().isPresent());
        assertEquals("arn:aws:kms:us-east-1:123456789012:key/test-key", context.getKeyArn().get());
        assertTrue(context.getDataKeyPair().isPresent());
        assertEquals("corp", context.getEncryptionContext().get("tenant"));
    }

    public void testIndexSettingsCryptoKeyProvider() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_STORE_CRYPTO_KEY_PROVIDER_SETTING.getKey(), "my-key-provider")
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_idx", settings);

        assertTrue(indexSettings.isStorageEncryptionEnabled());
        assertEquals("my-key-provider", indexSettings.getCryptoKeyProvider());

        Settings unencryptedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings unencryptedIndexSettings = IndexSettingsModule.newIndexSettings("test_idx_2", unencryptedSettings);

        assertFalse(unencryptedIndexSettings.isStorageEncryptionEnabled());
        assertEquals("", unencryptedIndexSettings.getCryptoKeyProvider());
    }

    public void testIndexingEngineConfigShardCryptoContext() {
        ShardCryptoContext context = new ShardCryptoContext(
            "test-provider",
            "test-type",
            null,
            null,
            Map.of()
        );

        IndexingEngineConfig config = new IndexingEngineConfig(
            null,
            null,
            null,
            null,
            null,
            Map.of(),
            context
        );

        assertNotNull(config.shardContext());
        assertEquals("test-provider", config.shardContext().getKeyProviderName());

        IndexSettings childSettings = IndexSettingsModule.newIndexSettings(
            "child_idx",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
        IndexingEngineConfig childConfig = config.childConfigFor(childSettings);

        assertNotNull(childConfig.shardContext());
        assertEquals("test-provider", childConfig.shardContext().getKeyProviderName());
        assertEquals("child_idx", childConfig.indexSettings().getIndex().getName());
    }

    public void testStoreStrategySupportsEncryption() {
        StoreStrategy strategy = new StoreStrategy() {
            @Override
            public Optional<org.opensearch.index.engine.dataformat.DataFormatStoreHandlerFactory> storeHandler() {
                return Optional.empty();
            }
        };

        assertTrue(strategy.supportsTransparentEncryption());
    }
}
