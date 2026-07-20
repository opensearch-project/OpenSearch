/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.engine;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.crypto.ShardCryptoContext;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class CryptoEnginePluginTests extends OpenSearchTestCase {

    public void testBuildShardCryptoContextWithMasterKeyProvider() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_STORE_CRYPTO_KEY_PROVIDER_SETTING.getKey(), "kms-provider")
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_idx", settings);

        MasterKeyProvider mockProvider = new MasterKeyProvider() {
            @Override
            public DataKeyPair generateDataPair() {
                return new DataKeyPair("enc".getBytes(), "dec".getBytes());
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey) {
                return encryptedKey;
            }

            @Override
            public String getKeyId() {
                return "key-123";
            }

            @Override
            public Map<String, String> getEncryptionContext() {
                return Map.of("env", "prod");
            }

            @Override
            public void close() {}
        };

        ShardCryptoContext cryptoContext = CryptoKeyUnwrapper.buildShardCryptoContext(indexSettings, mockProvider);
        assertNotNull(cryptoContext);
        assertEquals("kms-provider", cryptoContext.getKeyProviderName());
        assertTrue(cryptoContext.getDataKeyPair().isPresent());
        assertEquals("prod", cryptoContext.getEncryptionContext().get("env"));
    }

    public void testCryptoStoreDecoratorDecorate() throws IOException {
        CryptoStoreDecorator decorator = new CryptoStoreDecorator();
        Directory rawDirectory = new ByteBuffersDirectory();

        // Null crypto context returns raw directory unencrypted
        Directory decoratedUnencrypted = decorator.decorate(rawDirectory, null);
        assertSame(rawDirectory, decoratedUnencrypted);

        // Valid crypto context decorates handle
        ShardCryptoContext context = new ShardCryptoContext("kms", "type", null, null, Map.of());
        Directory decoratedEncrypted = decorator.decorate(rawDirectory, context);
        assertNotNull(decoratedEncrypted);
    }

    public void testPluginRegistersDirectoryFactory() {
        CryptoEnginePlugin plugin = new CryptoEnginePlugin();
        Map<String, IndexStorePlugin.DirectoryFactory> factories = plugin.getDirectoryFactories();
        assertTrue(factories.containsKey(CryptoEnginePlugin.STORE_TYPE));
        assertNotNull(factories.get(CryptoEnginePlugin.STORE_TYPE));
    }
}
