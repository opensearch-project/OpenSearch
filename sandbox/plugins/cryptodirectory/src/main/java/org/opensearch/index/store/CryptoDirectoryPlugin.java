/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A plugin that enables index level encryption and decryption.
 */
public class CryptoDirectoryPlugin extends Plugin implements IndexStorePlugin {

    /**
     * The default constructor.
     */
    public CryptoDirectoryPlugin() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CryptoDirectoryFactory.INDEX_KMS_TYPE_SETTING, CryptoDirectoryFactory.INDEX_CRYPTO_PROVIDER_SETTING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return java.util.Collections.singletonMap("cryptofs", new CryptoDirectoryFactory());
    }
}
