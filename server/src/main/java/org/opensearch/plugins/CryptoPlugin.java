/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.MasterKeyProvider;

/**
 * Crypto plugin to provide encryption and decryption support.
 * @opensearch.api
 */
@ExperimentalApi
public interface CryptoPlugin<T, U> {

    /**
     * To create a crypto handler for handling encryption and decryption ops.
     * @param keyProvider key provider instance to provide keys used in encrypting data.
     * @param keyProviderName Name of key provider to distinguish between multiple instances created with different
     *                        configurations of same keyProviderType.
     * @param keyProviderType Unique type of key provider to distinguish between different key provider implementations.
     * @param onClose Closes key provider or other clean up operations on close.
     * @return crypto handler instance.
     */
    CryptoHandler<T, U> getOrCreateCryptoHandler(
        MasterKeyProvider keyProvider,
        String keyProviderName,
        String keyProviderType,
        Runnable onClose
    );
}
