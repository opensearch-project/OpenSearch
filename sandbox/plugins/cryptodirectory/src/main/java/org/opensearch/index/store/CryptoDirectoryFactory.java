/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.function.Function;

/**
 * Factory for an encrypted filesystem directory
 */
public class CryptoDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    /**
     * Creates a new CryptoDirectoryFactory
     */
    public CryptoDirectoryFactory() {
        super();
    }

    /**
     *  Specifies a crypto provider to be used for encryption. The default value is SunJCE.
     */
    public static final Setting<Provider> INDEX_CRYPTO_PROVIDER_SETTING = new Setting<>("index.store.crypto.provider", "SunJCE", (s) -> {
        Provider p = Security.getProvider(s);
        if (p == null) {
            throw new IllegalArgumentException("unrecognized [index.store.crypto.provider] \"" + s + "\"");
        } else return p;
    }, Property.IndexScope, Property.InternalIndex);

    /**
     *  Specifies the Key management plugin type to be used. The desired KMS plugin should be installed.
     */
    public static final Setting<String> INDEX_KMS_TYPE_SETTING = new Setting<>(
        "index.store.kms.type",
        "",
        Function.identity(),
        Property.NodeScope,
        Property.IndexScope
    );

    /**
     * {@inheritDoc}
     * @param indexSettings the index settings
     * @param path the shard file path
     */
    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(org.opensearch.index.store.FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        final Provider provider = indexSettings.getValue(INDEX_CRYPTO_PROVIDER_SETTING);
        final String KEY_PROVIDER_TYPE = indexSettings.getValue(INDEX_KMS_TYPE_SETTING);
        final Settings settings = Settings.builder().put(indexSettings.getNodeSettings(), false).build();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(KEY_PROVIDER_TYPE, KEY_PROVIDER_TYPE, settings);
        MasterKeyProvider keyProvider = CryptoHandlerRegistry.getInstance()
            .getCryptoKeyProviderPlugin(KEY_PROVIDER_TYPE)
            .createKeyProvider(cryptoMetadata);
        return new CryptoDirectory(lockFactory, location, provider, keyProvider);
    }
}
