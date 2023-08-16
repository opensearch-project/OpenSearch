/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.RefCounted;

/**
 * Crypto plugin interface used for encryption and decryption.
 */
public interface CryptoManager extends RefCounted {

    /**
     * A factory interface for constructing crypto client.
     */
    interface Factory {

        /**
         * Constructs a crypto manager client based on provided key provider settings.
         *
         * @param cryptoSettings Settings needed for creating crypto client.
         * @param keyProviderName Name of the key provider.
         * @return instance of CryptoManager
         */
        CryptoManager create(Settings cryptoSettings, String keyProviderName);
    }

    /**
     * @return key provider type
     */
    String type();

    /**
     * @return key provider name
     */
    String name();

    /**
     * @return Crypto provider for encrypting or decrypting raw content.
     */
    CryptoProvider getCryptoProvider();
}
